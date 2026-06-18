<script setup>
import { computed, h, onBeforeUnmount, onMounted, ref, watch } from "vue";
import {
  Activity,
  Archive,
  Bell,
  Bot,
  CandlestickChart,
  CheckCircle2,
  Clock3,
  DatabaseZap,
  FileWarning,
  Gauge,
  LayoutDashboard,
  ListChecks,
  Play,
  RefreshCw,
  Send,
  Settings,
  ShieldCheck,
  SlidersHorizontal,
  Square,
  Target,
  WalletCards,
  Zap
} from "@lucide/vue";
import { api } from "./api";

const pages = [
  { key: "dashboard", label: "Dashboard", group: "Core", icon: LayoutDashboard },
  { key: "config", label: "Config", group: "Core", icon: SlidersHorizontal },
  { key: "micro", label: "Micro Daemon", group: "Core", icon: Bot },
  { key: "pipeline", label: "Pipeline", group: "Core", icon: Activity },
  { key: "snapshot", label: "Snapshot", group: "Core", icon: Gauge },
  { key: "plans", label: "Trade Plans", group: "Core", icon: CandlestickChart },
  { key: "strategy4", label: "Strategy4 / 异动肆号", group: "Core", icon: ListChecks },
  { key: "strategy5", label: "Strategy5 / 异动伍号", group: "Core", icon: Target },
  { key: "strategy6", label: "Strategy6 / 异动陆号", group: "Core", icon: Target },
  { key: "audit", label: "Audit", group: "Risk & Ops", icon: ShieldCheck },
  { key: "paper", label: "Paper Trading", group: "Risk & Ops", icon: WalletCards },
  { key: "trade-quality", label: "Trade Quality", group: "Risk & Ops", icon: Target },
  { key: "backtest-lab", label: "Backtest Lab", group: "Risk & Ops", icon: DatabaseZap },
  { key: "sandbox-lab", label: "Sandbox Lab", group: "Risk & Ops", icon: DatabaseZap },
  { key: "research-db", label: "Research DB", group: "Risk & Ops", icon: DatabaseZap },
  { key: "notifications", label: "Notifications", group: "Risk & Ops", icon: Send }
];

const PageHeader = {
  props: ["title", "subtitle"],
  setup(props) {
    return () =>
      h("div", { class: "hero compact" }, [
        h("div", {}, [
          h("h2", {}, props.title || ""),
          h("p", {}, props.subtitle || ""),
        ]),
      ]);
  }
};

const ConfigPanel = {
  props: ["title", "data"],
  setup(props) {
    return () =>
      h("article", { class: "card panel" }, [
        h("div", { class: "panel-header" }, [
          h("div", { class: "panel-title" }, [h("span", { class: "accent" }), props.title || "Config"]),
          h("span", { class: "tag blue" }, "YAML"),
        ]),
        h("div", { class: "panel-body" }, [
          h("pre", {}, JSON.stringify(props.data || {}, null, 2)),
        ]),
      ]);
  }
};

const activePage = ref(new URLSearchParams(window.location.search).get("page") || "dashboard");
const loading = ref(false);
const error = ref("");
const health = ref(null);
const config = ref({});
const configProfiles = ref({ active_profile: "custom", profiles: [] });
const configDrafts = ref({});
const configSaving = ref("");
const configMessage = ref("");
const configFieldImpactSummary = ref({});
const configUiSchema = ref({ groups: {}, tabs: [] });
const configEffective = ref({});
const configActiveTab = ref("strategy-runtime");
const tradePlans = ref({ lines: {} });
const tradePlanFunnel = ref({ strategy_lines: [], counts: {} });
const strategy4Runtime = ref({ status: {}, heartbeat: {}, pool: {} });
const strategy4ObservePool = ref({ items: [], status_counts: {} });
const strategy4Attempts = ref({ items: [] });
const strategy5Runtime = ref({ latest_trade_plan: {}, latest_evidence: {}, items: [] });
const strategy5Evidence = ref({ items: [] });
const strategy6Runtime = ref({ latest_trade_plan: {}, latest_evidence: {}, latest_decisions: {}, latest_wait_pool: {}, items: [] });
const strategy6Evidence = ref({ items: [] });
const strategy6Decisions = ref({ items: [] });
const strategy6WaitPool = ref({ items: [] });
const strategy6Attempts = ref({ items: [] });
const strategy6Heartbeat = ref({});
const strategy6Watchdog = ref({});
const selectedTradePlanLine = ref("");
const paper = ref({});
const paperStatus = ref({});
const paperConsumption = ref({});
const paperIntents = ref({ rows: [] });
const paperEpochs = ref({ rows: [] });
const paperExperiments = ref({ experiments: [] });
const paperRealism = ref({ metrics: {}, fills_sample: [] });
const paperReconciliation = ref({ counts: {}, fills: [], orders: [] });
const tradeQuality = ref({ summary: {}, aggregates: [], samples: [], phenomena: [], replay_ledger: [] });
const tradeQualityPackages = ref({ packages: [] });
const tradeQualitySyncStatus = ref({});
const showLegacyTradeQualityPanels = false;
const tradeQualityFilters = ref({
  source: "current_paper",
  archive_id: "",
  package_key: "",
  experiment_id: "",
  parameter_set_id: "",
  backtest_package_strategy_line: "",
  strategy_line: "all",
  side: "all",
  symbol: "",
  exit_reason: "all",
  root_cause: "all",
  entry_quality_label: "all",
  entry_quality_v2_label: "all",
  market_context_label: "all",
  market_context_status: "all",
  entry_context_v3_label: "all",
  funding_regime: "all",
  oi_direction: "all",
  btc_alignment: "all",
  microstructure_coverage: "all",
  replay_status: "all",
  quality_tag: "",
  limit: 50,
  offset: 0
});
const tradeQualityLoading = ref(false);
const tradeQualityDetailsLoading = ref(false);
const tradeQualityLazyMessage = ref("");
const tradeQualityRequestSeq = ref(0);
const tradeQualityIngestLoading = ref(false);
const tradeQualityIngest = ref({ summary: {}, latest: {}, ledger: [] });
const tradeQualityReplayLoading = ref(false);
const tradeQualityReplay = ref({ summary: {}, latest: {}, ledger: [] });
const tradeQualityEntryFeatureLoading = ref(false);
const tradeQualityEntryFeatureLast = ref(null);
const tradeQualityEntryMicroLoading = ref(false);
const tradeQualityEntryMicroLast = ref(null);
const tradeQualityRefreshBusy = ref(false);
const tradeQualityRefreshStage = ref("");
const tradeQualityRefreshLast = ref(null);
const tradeQualityRules = ref({ summary: {}, rules: [] });
const tradeQualityRulesLoading = ref(false);
const tradeQualityRuleFilters = ref({ rule_type: "all", sample_source: "all", strategy_line: "all", severity: "all", symbol: "" });
const tradeQualityValidation = ref({ summary: {}, matches: [] });
const tradeQualityValidationLoading = ref(false);
const tradeQualityPromotions = ref({ summary: {}, promotions: [] });
const tradeQualityPromotionLoading = ref(false);
const tradeQualityPromotionDraft = ref({ profile: "relaxed_profit", strategy_line: "all", mode: "wait_only" });
const tradeQualityPromotionPreview = ref(null);
const tradeQualitySelectedSample = ref(null);
const tradeQualityV4Loading = ref(false);
const tradeQualityV4Message = ref("");
const tradeQualityV4 = ref({ summary: {}, evidence: { rows: [], coverage: [] }, deep: { rollups: [] }, gates: { candidates: [] } });
const tradeQualityV5Loading = ref(false);
const tradeQualityV5Message = ref("");
const tradeQualityV5 = ref({ summary: {}, causal: { rows: [], rollups: [] }, gates: { candidates: [] }, coverage: {} });
const backtestLabLoading = ref(false);
const backtestLabAction = ref("");
const backtestLabFilters = ref({
  strategy_line: "all",
  days: 30,
  max_symbols: 20,
  max_sets: 120,
  symbol_shard_size: 25,
  max_workers: 6,
  scheduler_mode: "parameter_batch",
  symbols: ""
});
const backtestLabPackages = ref({ packages: [] });
const backtestLabBaseline = ref(null);
const backtestLabMatrix = ref(null);
const backtestLabExperiments = ref({ experiments: [] });
const backtestLabRecommendations = ref({ recommendations: [] });
const backtestLabKlineStatus = ref({ symbols: [] });
const backtestLabMatrixContract = ref({ parameter_sets: [] });
const backtestLabLeaderboard = ref({ leaderboard: [] });
const backtestLabMessage = ref("");
const backtestLabJobs = ref({ jobs: [] });
const backtestLabJob = ref(null);
const backtestLabJobTimer = ref(null);
const backtestLabDiscoveryTimer = ref(null);
const backtestLabSelectedExperiment = ref("");
const backtestLabExperimentDetail = ref(null);
const backtestLabExperimentOrders = ref({ orders: [] });
const backtestLabExperimentDaily = ref({ rows: [] });
const backtestLabExperimentSymbols = ref({ rows: [] });
const backtestLabQualityLoading = ref(false);
const backtestLabQualitySelection = ref(null);
const backtestLabQualitySummary = ref(null);
const backtestLabQualityAggregates = ref({ aggregates: [] });
const backtestLabQualitySamples = ref({ samples: [] });
const backtestLabQualityDryRun = ref(null);
const backtestLabStrategy4Replay = ref({ pool_counts: {}, metrics: [] });
const backtestLabStrategy4ReplayPool = ref({ pool: [] });
const backtestLabStrategy4ReplayAttempts = ref({ attempts: [] });
const backtestLabStrategy4ReplayRun = ref(null);
const backtestLabGateLoading = ref(false);
const backtestLabGateMessage = ref("");
const backtestLabGateFilters = ref({
  experiment_id: "",
  strategy_line: "all",
  parameter_set_id: "",
  top_n: 5,
  limit: 500,
  min_samples: 5,
  min_test_pf: 1.0,
  min_coverage: 0.05
});
const backtestLabGateBatch = ref(null);
const backtestLabGateFeatureBuild = ref(null);
const backtestLabGateBucketBuild = ref(null);
const backtestLabGateScoreBuild = ref(null);
const backtestLabGateCandidateBuild = ref(null);
const backtestLabGateFeatures = ref({ features: [] });
const backtestLabGateBuckets = ref({ buckets: [] });
const backtestLabGateScores = ref({ scores: [] });
const backtestLabGateCandidates = ref({ candidates: [] });
const backtestLabGateRecommendations = ref({ recommendations: [] });
const backtestLabTqJobs = ref({ jobs: [] });
const backtestLabTqJobLoading = ref(false);
const backtestLabTqJobMessage = ref("");
const sandboxLabLoading = ref(false);
const sandboxLabMessage = ref("");
const sandboxLabFilters = ref({ strategy_line: "all", status: "all", tag: "", limit: 100 });
const sandboxLabCreateDraft = ref({
  strategy_line: "experiment",
  strategy_lines: ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"],
  strategy_version: "review",
  data_scope: "{\"days\": 30, \"symbols\": []}",
  config_scope: "{\"mode\": \"shadow_only\"}",
  tags: "sandbox",
  set_active_after_create: false
});
const sandboxLabList = ref({ sandboxes: [], count: 0 });
const sandboxLabActive = ref({ active_sandbox_id: null, active: null });
const sandboxLabSummary = ref(null);
const sandboxLabHealth = ref(null);
const sandboxLabBranches = ref({ branches: [], count: 0 });
const sandboxLabLeaderboard = ref({ leaderboard: [], count: 0 });
const sandboxLabTqCompare = ref({ items: [], count: 0 });
const sandboxLabGateCompare = ref({ items: [], count: 0 });
const sandboxLabCodeOverlay = ref(null);
const sandboxLabSelectedBranch = ref("strategy5");
const sandboxLabPatchDraft = ref({
  target_relpath: "notes/strategy-experiment.md",
  note: "sandbox-only strategy logic experiment note",
  diff_text: ""
});
const sandboxLabLastJob = ref(null);
const sandboxLabSelectedId = ref("");
const researchDbLoading = ref(false);
const researchDbMessage = ref("");
const researchDbFilters = ref({ strategy_line: "all", source_type: "all", limit: 100 });
const researchDb = ref({
  summary: { counts: {}, strategies: [], source_types: [], feature_quality: {} },
  tradeFacts: { rows: [], total: 0 },
  entryFeatures: { rows: [], total: 0 },
  tqSamples: { rows: [], total: 0 },
  datasetCards: { cards: [] },
  writerStatus: { writers: [], materialize_ledger: [], native_contract: {} },
  fieldCoverage: { missing_fields_top: [], proxy_fields_top: [] },
  lineageAudit: { status: "unknown" }
});
const paperAutoRefreshTimer = ref(null);
const paperLastRefreshAt = ref("");
const paperNextRefreshAt = ref("");
const paperArchiveBusy = ref("");
const paperArchiveMessage = ref("");
const feishu = ref({});
const deliveries = ref([]);
const pipeline = ref({});
const pipelineWatchdog = ref({});
const restHealth = ref({});
const runtimeRestBudget = ref({});
const step15Daemon = ref({});
const step15DaemonHealth = ref({});
const runtimeWarmup = ref({});
const micro = ref({});
const audit = ref({});
const runAudit = ref({});
const runAuditList = ref({ runs: [] });
const microQualityAudit = ref({ symbols: [], summary: {} });
const microEvidenceRuntime = ref({ symbols: [], summary: {} });
const microEvidenceTargetSource = ref({ targets: [], target_source_distribution: {} });
const microTrainingLatest = ref({ run_samples: [], symbols: [] });
const microTrainingRuns = ref({ runs: [] });
const microTrainingCoverage = ref({});
const microFullZAudit = ref({ symbols: [], summary: {} });
const microFastRuntimeAudit = ref({ symbols: [], summary: {} });
const microFastTailCleanupAudit = ref({ symbols: [], summary: {} });
const microFastJudgeableAudit = ref({ symbols: [], summary: {} });
const microFastJudgeableOnlyAudit = ref({ symbols: [], summary: {} });
const microFastJudgeableThroughputAudit = ref({ symbols: [], summary: {} });
const microFastCoverageSplitAudit = ref({ symbols: [], summary: {} });
const microFastValidBucketAudit = ref({ symbols: [], summary: {} });
const candidateGovernance = ref({ items: [], counts: {} });
const auditLineFilter = ref("all");
const auditCoreLoading = ref(false);
const auditSideLoading = ref(false);
const auditSideLoadedRunId = ref("");
const auditLazyError = ref("");
const auditLazySeq = ref(0);
const auditDetailsRequested = ref(false);
const showAuditRawPayload = ref(false);
const runtime = ref({});
const paperTab = ref("overview");
const detailModal = ref(null);
const runtimeLog = ref([]);
const pipelinePollTimer = ref(null);
const pipelinePollInFlight = ref(false);
const pipelineFunnel = ref({ lines: {}, items: [], summary: {} });
const pageLoadSeq = ref(0);
const selectedPipelineLines = ref(["without_micro", "micro_fast", "strategy5", "strategy6"]);
const pipelineUi = ref({
  mode: "idle",
  cycleCount: 0,
  onceDone: 0,
  loopDone: 0,
  overall: 0,
  lines: {
    without_micro: { percent: 0, stage: "waiting" },
    micro_fast: { percent: 0, stage: "waiting" },
    micro_full: { percent: 0, stage: "waiting" },
    strategy5: { percent: 0, stage: "waiting" },
    strategy6: { percent: 0, stage: "waiting" }
  },
  logs: ["pipeline ready · choose Run Once or Run Cycle"]
});

const pipelineStages = [
  { code: "P1", name: "Universe" },
  { code: "P2", name: "Snapshot" },
  { code: "P3", name: "Micro" },
  { code: "P4", name: "Plan" },
  { code: "P5", name: "Paper" },
  { code: "P6", name: "Notify" }
];

const pipelineStrategyMeta = [
  {
    line: "without_micro",
    title: "without_micro",
    desc: "基础链路，不等待微观确认，优先跑完 plan 与结果产出。",
    className: ""
  },
  {
    line: "micro_fast",
    title: "micro_fast",
    desc: "快速微观确认，盘口就绪后推进执行与模拟。",
    className: "fast"
  },
  {
    line: "micro_full",
    title: "micro_full",
    desc: "完整微观确认，等待 OFI / CVD / depth 更充分对齐。",
    className: "full"
  },
  {
    line: "strategy5",
    title: "strategy5",
    desc: "方向证据支线，复用现有证据，不占 micro slot，进入 paper 对比。",
    className: "strategy5"
  },
  {
    line: "strategy6",
    title: "strategy6",
    desc: "市场接受入场支线，二次确认方向和入场价格。",
    className: "strategy6"
  }
];

const corePages = computed(() => pages.filter((page) => page.group === "Core"));
const riskPages = computed(() => pages.filter((page) => page.group === "Risk & Ops"));
const currentPage = computed(() => pages.find((page) => page.key === activePage.value));
const sandboxLabRows = computed(() => (sandboxLabList.value?.sandboxes || []).filter((item) => item.status !== "deleted"));
const sandboxLabSelected = computed(() => sandboxLabRows.value.find((item) => item.sandbox_id === sandboxLabSelectedId.value) || null);
const sandboxLabLastJobStatus = computed(() => String(sandboxLabSummary.value?.last_job?.status || sandboxLabSelected.value?.last_job_status || "").toLowerCase());
const sandboxLabJobRunning = computed(() => ["queued", "running", "manifest_ready"].includes(sandboxLabLastJobStatus.value));
const sandboxLabNoSelection = computed(() => !sandboxLabSelectedId.value);
const sandboxLabActionDisabled = computed(() => sandboxLabNoSelection.value || sandboxLabLoading.value || sandboxLabJobRunning.value);
const tradeLines = computed(() => tradePlans.value?.lines || {});
const strategy4Status = computed(() => strategy4Runtime.value?.status || {});
const strategy4Heartbeat = computed(() => strategy4Runtime.value?.heartbeat || {});
const strategy4Pool = computed(() => strategy4Runtime.value?.pool || {});
const strategy4PoolCounts = computed(() => ({
  ...(strategy4Pool.value?.status_counts || {}),
  ...(strategy4ObservePool.value?.status_counts || {})
}));
const strategy4PoolRows = computed(() => strategy4ObservePool.value?.items || []);
const strategy4AttemptRows = computed(() => strategy4Attempts.value?.items || []);
const strategy4PlanDoc = computed(() => tradeLines.value?.strategy4 || {});
const strategy4PlanSummary = computed(() => {
  const doc = strategy4PlanDoc.value || {};
  const fresh = doc.output_fresh !== false;
  return {
    count: fresh ? Number(doc.count || 0) : 0,
    executable: Number(doc.effective_executable_count ?? (fresh ? doc.executable_count : 0) ?? 0),
    generated_at: doc.generated_at || doc.output_generated_at || "-",
    output_run_id: doc.output_run_id || doc.run_id || "-",
    stale: !fresh,
    stale_reason: doc.stale_output_reason || ""
  };
});
const strategy4PoolCount = computed(() => Number(strategy4ObservePool.value?.count ?? strategy4Pool.value?.count ?? 0));
const strategy4RuntimeState = computed(() => strategy4Status.value?.state || strategy4ObservePool.value?.status || "unknown");
const strategy4DebugPayload = computed(() => ({
  runtime: strategy4Runtime.value,
  observe_pool: strategy4ObservePool.value,
  attempts: strategy4Attempts.value
}));
const strategy5PlanDoc = computed(() => tradeLines.value?.strategy5 || {});
const strategy5RuntimePlan = computed(() => strategy5Runtime.value?.latest_trade_plan || {});
const strategy5EvidenceRows = computed(() => strategy5Evidence.value?.items || strategy5Runtime.value?.items || []);
const strategy5Summary = computed(() => ({
  status: strategy5RuntimePlan.value?.status || strategy5PlanDoc.value?.status || "unknown",
  count: Number(strategy5RuntimePlan.value?.count ?? strategy5PlanDoc.value?.count ?? 0),
  executable: Number(strategy5RuntimePlan.value?.executable_count ?? strategy5PlanDoc.value?.executable_count ?? 0),
  evidence: Number(strategy5Runtime.value?.latest_evidence?.count ?? strategy5Evidence.value?.count ?? 0),
  generated_at: strategy5RuntimePlan.value?.generated_at || strategy5PlanDoc.value?.generated_at || "-"
}));
const strategy6PlanDoc = computed(() => tradeLines.value?.strategy6 || {});
const strategy6RuntimePlan = computed(() => strategy6Runtime.value?.latest_trade_plan || {});
const strategy6EvidenceRows = computed(() => strategy6Evidence.value?.items || strategy6Runtime.value?.items || []);
const strategy6DecisionRows = computed(() => strategy6Decisions.value?.items || []);
const strategy6WaitRows = computed(() => strategy6WaitPool.value?.items || []);
const strategy6AttemptRows = computed(() => strategy6Attempts.value?.items || []);
const strategy6Daemon = computed(() => strategy6Runtime.value?.daemon || strategy6Heartbeat.value || {});
const strategy6WatchdogStatus = computed(() => strategy6Watchdog.value?.watchdog_status || strategy6Daemon.value?.watchdog_status || "unknown");
const strategy6Summary = computed(() => ({
  status: strategy6Daemon.value?.status || strategy6RuntimePlan.value?.status || strategy6PlanDoc.value?.status || "unknown",
  count: Number(strategy6RuntimePlan.value?.count ?? strategy6PlanDoc.value?.count ?? 0),
  executable: Number(strategy6RuntimePlan.value?.executable_count ?? strategy6PlanDoc.value?.executable_count ?? 0),
  evidence: Number(strategy6Runtime.value?.latest_evidence?.count ?? strategy6Evidence.value?.count ?? 0),
  decisions: Number(strategy6Runtime.value?.latest_decisions?.count ?? strategy6Decisions.value?.count ?? 0),
  wait: Number(strategy6Runtime.value?.latest_wait_pool?.count ?? strategy6WaitPool.value?.count ?? 0),
  attempts: Number(strategy6Attempts.value?.count ?? strategy6AttemptRows.value.length ?? 0),
  generated_at: strategy6RuntimePlan.value?.generated_at || strategy6PlanDoc.value?.generated_at || "-"
}));
const pipelineRunControls = computed(() => pipeline.value?.run_controls || {});
const watchdogHealthClass = computed(() => {
  const health = pipelineWatchdog.value?.health;
  if (health === "ok") return "good";
  if (health === "fail") return "bad";
  return "warn";
});
const watchdogStateText = computed(() => {
  const state = pipelineWatchdog.value?.display_state || pipelineWatchdog.value?.scheduler_status || "-";
  return String(state).replaceAll("_", " ");
});
const isCycleWaiting = computed(() =>
  pipeline.value?.display_state === "interval_waiting"
  || pipeline.value?.active_interval?.status === "interval_waiting"
  || pipelineWatchdog.value?.display_state === "interval_waiting"
);
const pipelineRegistryBusy = computed(() => {
  const health = pipeline.value?.registry_health || {};
  return Boolean(
    health.pid_running
    || health.registry_pid_running
    || health.lock_pid_running
    || pipeline.value?.lock_stale_but_pid_running
  );
});
const pipelineProgressRunning = computed(() => {
  const status = String(pipeline.value?.progress?.status || "").toLowerCase();
  return ["running", "starting", "interval_waiting"].includes(status)
    && Boolean(pipeline.value?.job_running || pipeline.value?.active_job || pipelineRunControls.value?.can_stop === true);
});
const pipelineTrulyIdle = computed(() =>
  Boolean(pipeline.value)
  && !pipeline.value?.job_running
  && !pipeline.value?.active_job
  && !pipeline.value?.active_interval
  && !isCycleWaiting.value
  && !pipelineRegistryBusy.value
  && !pipelineProgressRunning.value
  && pipelineRunControls.value?.can_stop !== true
);
const isPipelineRunning = computed(() =>
  Boolean(
    pipelineRunControls.value?.can_stop === true
    || pipeline.value?.job_running
    || pipeline.value?.active_job
    || pipeline.value?.active_interval
    || isCycleWaiting.value
    || pipelineRegistryBusy.value
  )
);
const baseCanSubmitPipeline = computed(() =>
  !loading.value
  && selectedPipelineLines.value.length > 0
);
const canRunOnce = computed(() =>
  baseCanSubmitPipeline.value
  && pipelineTrulyIdle.value
  && pipelineRunControls.value?.can_run_once !== false
);
const canStartCycle = computed(() =>
  baseCanSubmitPipeline.value
  && pipelineTrulyIdle.value
  && pipelineRunControls.value?.can_run_cycle !== false
);
const canStopPipeline = computed(() =>
  Boolean(pipelineRunControls.value?.can_stop === true)
);
const cycleToggleDisabled = computed(() =>
  isPipelineRunning.value ? !canStopPipeline.value || loading.value : !canStartCycle.value
);
const pipelineDisabledReason = computed(() => {
  if (loading.value) return "loading";
  if (selectedPipelineLines.value.length === 0) return "no strategy selected";
  if (!pipelineTrulyIdle.value) return "pipeline not idle";
  const reason = pipelineRunControls.value?.disabled_reason;
  if (!reason) return "";
  const labels = {
    pipeline_already_running: "pipeline running",
    pipeline_interval_active: "cycle active",
    interval_cycle_waiting: "cycle waiting",
    snapshot_warmup_not_ready: "snapshot warmup",
    pipeline_run_disabled: "run disabled"
  };
  return labels[reason] || String(reason).replaceAll("_", " ");
});
const runOnceDisabledReason = computed(() => (canRunOnce.value ? "" : pipelineDisabledReason.value));
const runCycleDisabledReason = computed(() => (canStartCycle.value || isPipelineRunning.value ? "" : pipelineDisabledReason.value));
const pipelineStatusLabel = computed(() => {
  if (pipeline.value?.job_running) return "running";
  if (isCycleWaiting.value) return `waiting next ${pipeline.value?.next_cycle_eta_sec ?? pipelineWatchdog.value?.next_cycle_eta_sec ?? "-"}s`;
  if (pipeline.value?.latest_report?.status) return String(pipeline.value.latest_report.status);
  return "idle";
});
const cycleButtonText = computed(() => (isPipelineRunning.value ? "Stop Full Chain Cycle" : "Run Full Chain Cycle"));
const lineNames = {
  without_micro: "异动壹号",
  micro_fast: "异动贰号",
  micro_full: "异动叁号",
  strategy4: "异动肆号",
  strategy5: "异动伍号",
  strategy6: "异动陆号"
};
const pipelineStrategyLineOrder = ["without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"];
const strategyLineOrder = ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"];
const selectedPipelineLineLabels = computed(() =>
  selectedPipelineLines.value.map((line) => lineNames[line] || line).join(" / ")
);
const configSections = [
  { key: "strategy-pipeline", title: "System / Strategy Pipeline", path: ["strategy_pipeline"], tabs: ["strategy-runtime", "advanced-legacy"] },
  { key: "market-entry-liquidity", title: "Liquidity / Market Entry", path: ["market_entry_liquidity"], tabs: ["strategy-runtime", "advanced-legacy"] },
  { key: "decision-refresh", title: "Freshness / Range Room", path: ["decision_refresh"], tabs: ["strategy-runtime", "advanced-legacy"] },
  { key: "paper", title: "Paper Daemon / Ledger", path: ["paper"], tabs: ["strategy-runtime", "advanced-legacy"] },
  { key: "feishu", title: "Feishu Notification", path: ["feishu"], tabs: ["advanced-legacy"] }
];
const visibleConfigSections = computed(() =>
  configSections.filter((section) => (section.tabs || ["advanced-legacy"]).includes(configActiveTab.value))
);
const configLineSections = ["without_micro", "micro_fast", "micro_full", "strategy5", "strategy6"];
const configGovernanceTabs = [
  { key: "strategy-runtime", label: "Strategy Runtime", hint: "run lines, profile, daemon and micro consumption" },
  { key: "entry-executable", label: "Entry & Executable", hint: "fields that can make a trade plan executable or blocked" },
  { key: "exit-rr", label: "Exit / RR", hint: "TP target policy, RR and fast-exit controls" },
  { key: "trade-gate", label: "Trade Gate", hint: "V5 paper gate and legacy TQ/SLTP shield" },
  { key: "advanced-legacy", label: "Advanced / Legacy", hint: "raw JSON, disabled fields, low-level runtime details" }
];
const microConsumptionPolicies = [
  { value: "confirmed_only", label: "confirmed only" },
  { value: "ready_signal_usable", label: "ready + signal usable" },
  { value: "weak_ready_test", label: "weak ready test" },
  { value: "audit_only", label: "audit only" }
];
const weakMicroMinStates = [
  { value: "ready", label: "ready" },
  { value: "signal_usable", label: "signal usable" }
];
const tradeQualityGateModes = [
  { value: "off", label: "off" },
  { value: "shadow", label: "shadow" },
  { value: "warn", label: "warn" },
  { value: "wait_only", label: "wait only" },
  { value: "block_executable", label: "block executable" }
];
const slTpQualityModes = [
  { value: "off", label: "off" },
  { value: "shadow", label: "shadow" },
  { value: "warn", label: "warn" },
  { value: "apply", label: "apply" }
];
const tpTargetPolicyModes = [
  { value: "structure", label: "structure" },
  { value: "fast_capped_rr", label: "fast capped RR" },
  { value: "structure_or_capped_rr", label: "structure or capped RR" }
];
const tpTargetPolicyBasisModes = [
  { value: "gross", label: "gross stop R" },
  { value: "net", label: "net R parity" }
];
const tpTargetPolicySizingBasisModes = [
  { value: "gross_stop", label: "gross stop" },
  { value: "net_planned_loss", label: "net planned loss" }
];
const tradeQualityGateNumberFields = [
  { key: "min_samples_per_symbol", label: "min symbol samples", step: "1" },
  { key: "min_samples_per_root_cause", label: "min cause samples", step: "1" },
  { key: "max_negative_expectancy_R", label: "max negative R", step: "0.1" }
];
const slTpQualityNumberFields = [
  { key: "min_samples_per_cluster", label: "min cluster samples", step: "1" },
  { key: "stop_too_tight_widen_factor", label: "widen stop factor", step: "0.01" },
  { key: "tp_too_far_reduce_factor", label: "reduce TP factor", step: "0.01" }
];
const tpTargetPolicyNumberFields = [
  { key: "target_rr", label: "target RR", step: "0.01" },
  { key: "target_rr_cap", label: "target RR cap", step: "0.01" },
  { key: "target_net_rr", label: "target net R", step: "0.01" },
  { key: "min_target_net_rr", label: "min net R", step: "0.01" },
  { key: "max_target_net_rr", label: "max net R", step: "0.01" },
  { key: "min_reward_bps", label: "min reward bps", step: "1" },
  { key: "market_room_buffer_bps", label: "room buffer bps", step: "1" },
  { key: "reward_to_spread_min", label: "reward / spread min", step: "0.1" },
  { key: "slippage_reserve_bps", label: "slippage reserve bps", step: "1" },
  { key: "max_loss_net_r", label: "max loss net R", step: "0.01" }
];
const marketNowSides = [
  { key: "long", label: "LONG NOW" },
  { key: "short", label: "SHORT NOW" }
];
const marketNowNumberFields = [
  { key: "min_range_pos", label: "min range" },
  { key: "max_range_pos", label: "max range" },
  { key: "min_available_room_bps", label: "min room bps" },
  { key: "max_stop_bps", label: "max stop bps" },
  { key: "max_stop_atr_mult", label: "max stop ATR" },
  { key: "min_net_rr", label: "min net RR" },
  { key: "max_spread_bps", label: "max spread bps" },
  { key: "max_slippage_bps", label: "max slippage bps" }
];
const planSummary = computed(() =>
  strategyLineOrder.map((line) => {
    const doc = tradeLines.value[line] || {};
    const fresh = doc.output_fresh !== false;
    return {
      line,
      name: lineNames[line],
      count: fresh ? Number(doc.count || 0) : 0,
      executable: Number(doc.effective_executable_count ?? (fresh ? doc.executable_count : 0) ?? 0),
      generated_at: doc.generated_at || "-",
      stale: !fresh,
      stale_reason: doc.stale_output_reason || "",
      output_run_id: doc.output_run_id || doc.run_id || null,
      display_run_id: doc.display_run_id || tradePlans.value?.display_run_id || null
    };
  })
);
const tradePlanFunnelLines = computed(() => tradePlanFunnel.value?.strategy_lines || []);
const selectedTradePlanLineDoc = computed(() => {
  const rows = tradePlanFunnelLines.value;
  if (!rows.length) return null;
  return rows.find((row) => row.line === selectedTradePlanLine.value) || rows.find((row) => !row.skipped) || rows[0];
});
const tradePlanFunnelCounts = computed(() => tradePlanFunnel.value?.counts || {});
const tradePlanReasonGroups = computed(() => selectedTradePlanLineDoc.value?.reason_groups || []);
const tradePlanSymbolRows = computed(() => selectedTradePlanLineDoc.value?.symbols || []);
const tradePlanFunnelStages = computed(() => selectedTradePlanLineDoc.value?.funnel || []);
const tradePlanRunLabel = computed(() => tradePlanFunnel.value?.run_id || tradePlans.value?.display_run_id || "-");
const tradePlanSelectedLineCounts = computed(() => selectedTradePlanLineDoc.value?.counts || {});
const paperStats = computed(() => paper.value?.stats?.by_line || {});
const recentFailures = computed(() => deliveries.value.filter((row) => row.status === "failed").length);
const latestDelivery = computed(() => deliveries.value[deliveries.value.length - 1] || null);
const freshnessRows = computed(() => [
  {
    source: "latest_trade_plan_without_micro",
    age: ageLabel(tradeLines.value.without_micro?.generated_at),
    status: tradeLines.value.without_micro?.output_fresh === false
      ? "stale"
      : freshnessStatus(tradeLines.value.without_micro?.generated_at),
    path: "DATA/decisions/latest_trade_plan_without_micro.json"
  },
  {
    source: "latest_trade_plan_micro_fast",
    age: ageLabel(tradeLines.value.micro_fast?.generated_at),
    status: tradeLines.value.micro_fast?.output_fresh === false
      ? "stale"
      : freshnessStatus(tradeLines.value.micro_fast?.generated_at),
    path: "DATA/decisions/latest_trade_plan_micro_fast.json"
  },
  {
    source: "latest_trade_plan_micro_full",
    age: ageLabel(tradeLines.value.micro_full?.generated_at),
    status: tradeLines.value.micro_full?.output_fresh === false
      ? "stale"
      : freshnessStatus(tradeLines.value.micro_full?.generated_at),
    path: "DATA/decisions/latest_trade_plan_micro_full.json"
  },
  {
    source: "latest_trade_plan_strategy4",
    age: ageLabel(tradeLines.value.strategy4?.generated_at),
    status: tradeLines.value.strategy4?.output_fresh === false
      ? "stale"
      : freshnessStatus(tradeLines.value.strategy4?.generated_at),
    path: "DATA/decisions/latest_trade_plan_strategy4.json"
  },
  {
    source: "latest_trade_plan_strategy6",
    age: ageLabel(tradeLines.value.strategy6?.generated_at),
    status: tradeLines.value.strategy6?.output_fresh === false
      ? "stale"
      : freshnessStatus(tradeLines.value.strategy6?.generated_at),
    path: "DATA/decisions/latest_trade_plan_strategy6.json"
  }
]);
const currentRunIds = computed(() => {
  const ids = Object.values(tradeLines.value || {})
    .map((doc) => doc?.run_id)
    .filter(Boolean);
  return new Set(ids);
});
const allPaperOrders = computed(() => {
  if (paperTab.value === "overview") {
    return Object.values(paper.value?.orders || {}).flat();
  }
  return paper.value?.orders?.[paperTab.value] || [];
});
const allPaperPositions = computed(() => {
  if (paperTab.value === "overview") {
    return Object.values(paper.value?.positions || {}).flat();
  }
  return paper.value?.positions?.[paperTab.value] || [];
});
const activeOrders = computed(() => allPaperOrders.value.filter(isMarketExecutableOrder));
const activePositions = computed(() => allPaperPositions.value);
const openPositionRows = computed(() => {
  const rows = rowsForLine(paper.value?.open_positions, paperTab.value);
  const source = rows.length ? rows : activePositions.value.filter((row) => row.status === "open");
  return sortByTime(source, ["opened_at", "updated_at"]);
});
const settledRows = computed(() => {
  const rows = rowsForLine(paper.value?.closed_orders, paperTab.value);
  const source = rows.length ? rows : activeOrders.value.filter((row) => row.status === "closed");
  return sortByTime(source, ["closed_at", "updated_at"]);
});
const skippedSignalRows = computed(() => {
  const rows = paper.value?.skipped_signals || [];
  return currentRunRows(paperTab.value === "overview" ? rows : rows.filter((row) => row.strategy_line === paperTab.value || row.line === paperTab.value));
});
const paperSkippedTotal = computed(() => {
  if (paperTab.value !== "overview") return skippedSignalRows.value.length;
  const count = paper.value?.counts?.skipped_signals;
  return Number.isFinite(Number(count)) ? Number(count) : skippedSignalRows.value.length;
});
const selectedViewStats = computed(() => viewStatsForLine(paperTab.value));
const paperRealismMetrics = computed(() => paperRealism.value?.metrics || {});
const paperReconciliationCounts = computed(() => paperReconciliation.value?.counts || {});
const paperRecentRealismFills = computed(() => {
  const rows = paperReconciliation.value?.fills || paperRealism.value?.fills_sample || [];
  if (paperTab.value === "overview") return rows;
  return rows.filter((row) => row.strategy_line === paperTab.value);
});
const paperExperimentRows = computed(() => {
  const rows = paperExperiments.value?.experiments || [];
  if (paperTab.value === "overview") return rows;
  return rows.filter((row) => row.strategy_line === paperTab.value);
});
const paperIntentRows = computed(() => {
  const rows = paperIntents.value?.rows || paper.value?.intent_inbox || [];
  if (paperTab.value === "overview") return rows;
  return rows.filter((row) => row.strategy_line === paperTab.value);
});
const paperEpochRows = computed(() => {
  const rows = paperEpochs.value?.rows || paper.value?.reset_epochs || [];
  if (paperTab.value === "overview") return rows;
  return rows.filter((row) => row.strategy_line === paperTab.value);
});
const tradeQualitySummary = computed(() => tradeQuality.value?.summary || {});
const tradeQualityPerformanceStats = computed(() => tradeQualitySummary.value?.performance_stats || {});
const tradeQualityRootCauseAttribution = computed(() => tradeQualitySummary.value?.root_cause_attribution || {});
const tradeQualityRootCauseAttributionItems = computed(() => tradeQualityRootCauseAttribution.value?.items || []);
const tradeQualityDimensionAttribution = computed(() => tradeQualitySummary.value?.dimension_attribution || {});
const tradeQualityDimensionSymbolRows = computed(() => tradeQualityDimensionAttribution.value?.symbol || []);
const tradeQualityDimensionHourRows = computed(() => tradeQualityDimensionAttribution.value?.hour_bucket || []);
const tradeQualityDimensionHoldingRows = computed(() => tradeQualityDimensionAttribution.value?.holding_bucket || []);
const tradeQualityDimensionSideRows = computed(() => tradeQualityDimensionAttribution.value?.side || []);
const tradeQualityDimensionMarketContext = computed(() => tradeQualityDimensionAttribution.value?.market_context || {});
const tradeQualityEntryQualityAttribution = computed(() => tradeQualitySummary.value?.entry_quality_attribution || {});
const tradeQualityEntryQualityRows = computed(() => tradeQualityEntryQualityAttribution.value?.items || []);
const tradeQualityEntryMicroAttribution = computed(() => tradeQualitySummary.value?.entry_microstructure_attribution || {});
const tradeQualityEntryMicroRows = computed(() => tradeQualityEntryMicroAttribution.value?.items || []);
const tradeQualityEntryMicroStrategyRows = computed(() => tradeQualityEntryMicroAttribution.value?.by_strategy_line || []);
const tradeQualityEntryMarketAttribution = computed(() => tradeQualitySummary.value?.entry_market_context_attribution || {});
const tradeQualityEntryMarketRows = computed(() => tradeQualityEntryMarketAttribution.value?.items || []);
const tradeQualityEntryV3Attribution = computed(() => tradeQualitySummary.value?.entry_context_v3_attribution || {});
const tradeQualityEntryV3Rows = computed(() => tradeQualityEntryV3Attribution.value?.items || []);
const tradeQualityEntryV3StrategyRows = computed(() => tradeQualityEntryV3Attribution.value?.by_strategy_line || []);
const tradeQualitySamples = computed(() => tradeQuality.value?.samples || []);
const tradeQualityAggregates = computed(() => tradeQuality.value?.aggregates || []);
const tradeQualityPhenomena = computed(() => tradeQuality.value?.phenomena || tradeQualitySummary.value?.phenomena || []);
const tradeQualityReplayLedgerRows = computed(() => tradeQuality.value?.replay_ledger || tradeQualityReplay.value?.ledger || []);
const tradeQualityRecommendations = computed(() => tradeQuality.value?.recommendations || []);
const tradeQualityArchiveIngest = computed(() => tradeQuality.value?.archive_ingest || tradeQualityIngest.value?.summary || {});
const tradeQualityArchiveLatest = computed(() => tradeQualityIngest.value?.latest || {});
const tradeQualityLedgerRows = computed(() => tradeQualityIngest.value?.ledger || []);
const tradeQualityReplaySummary = computed(() => tradeQuality.value?.replay_backfill || tradeQualityReplay.value?.summary || {});
const tradeQualityReplayLatest = computed(() => tradeQualityReplay.value?.latest || {});
const tradeQualityRuleRows = computed(() => tradeQualityRules.value?.rules || []);
const tradeQualityRuleSummary = computed(() => tradeQualityRules.value?.summary || {});
const tradeQualityValidationRows = computed(() => tradeQualityValidation.value?.matches || []);
const tradeQualityValidationSummary = computed(() => tradeQualityValidation.value?.summary || {});
const tradeQualityPromotionRows = computed(() => tradeQualityPromotions.value?.promotions || []);
const tradeQualityV4Summary = computed(() => tradeQualityV4.value?.summary || {});
const tradeQualityV4CoverageRows = computed(() => tradeQualityV4.value?.evidence?.coverage || []);
const tradeQualityV4DeepRows = computed(() => tradeQualityV4.value?.deep?.rollups || []);
const tradeQualityV4GateRows = computed(() => tradeQualityV4.value?.gates?.candidates || []);
const tradeQualityV5Summary = computed(() => tradeQualityV5.value?.summary || {});
const tradeQualityV5CausalRows = computed(() => tradeQualityV5.value?.causal?.rows || []);
const tradeQualityV5RollupRows = computed(() => tradeQualityV5.value?.causal?.rollups || []);
const tradeQualityV5GateRows = computed(() => tradeQualityV5.value?.gates?.candidates || []);
const tradeQualityV5Coverage = computed(() => tradeQualityV5.value?.coverage || {});
const tradeQualityV5CoverageRows = computed(() => tradeQualityV5Coverage.value?.causal_source_quality || []);
const tradeQualityRootCauses = computed(() =>
  tradeQualityAggregates.value.filter((row) => row.dimension === "root_cause").sort((a, b) => Number(a.avg_net_R || 0) - Number(b.avg_net_R || 0))
);
const tradeQualityPackageRows = computed(() => tradeQualityPackages.value?.packages || []);
const selectedTradeQualityPackage = computed(() => {
  if (tradeQualityFilters.value.source === "backtest_p21_v2") {
    const packageKey = String(tradeQualityFilters.value.package_key || "");
    return tradeQualityPackageRows.value.find((row) => row.package_key === packageKey) || null;
  }
  const archiveId = String(tradeQualityFilters.value.archive_id || "");
  return tradeQualityPackageRows.value.find((row) => row.archive_id === archiveId) || null;
});
const tradeQualityCanPrev = computed(() => Number(tradeQualityFilters.value.offset || 0) > 0);
const tradeQualityCanNext = computed(() =>
  Number(tradeQualityFilters.value.offset || 0) + Number(tradeQualityFilters.value.limit || 200) < Number(tradeQuality.value?.total || 0)
);
const tradeQualityByStrategy = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "strategy_line"));
const tradeQualityBySide = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "side"));
const tradeQualityByExitReason = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "exit_reason"));
const tradeQualityByTag = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "quality_tag").slice(0, 24));
const tradeQualityStrategySideRows = computed(() => [
  ...tradeQualityByStrategy.value,
  ...tradeQualityBySide.value
]);
const tradeQualityBySymbol = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "symbol").slice(0, 30));
const tradeQualityMarketContextAggregates = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "market_context_label"));
const tradeQualityEntryV3Aggregates = computed(() => tradeQualityAggregates.value.filter((row) => row.dimension === "entry_context_v3_label"));
const microHealth = computed(() => runtime.value?.micro_daemon || {});
const paperHealth = computed(() => runtime.value?.paper_daemon || {});
const nonEmptyObject = (...values) => values.find((value) => value && typeof value === "object" && Object.keys(value).length > 0) || {};
const snapshotDaemonPayload = computed(() => nonEmptyObject(step15Daemon.value));
const snapshotDaemonHealthPayload = computed(() => nonEmptyObject(step15DaemonHealth.value));
const snapshotDaemonSummary = computed(() => nonEmptyObject(snapshotDaemonHealthPayload.value, snapshotDaemonPayload.value, restHealth.value?.snapshot_daemon));
const snapshotLatest = computed(() => nonEmptyObject(snapshotDaemonPayload.value?.latest_snapshot, restHealth.value?.latest_snapshot));
const snapshotRows = computed(() => {
  const rows = snapshotDaemonPayload.value?.items
    || snapshotDaemonPayload.value?.latest_snapshot?.items
    || snapshotDaemonPayload.value?.latest_snapshot?.raw?.items
    || snapshotLatest.value?.items
    || [];
  return Array.isArray(rows) ? rows : [];
});
const snapshotFreshnessCounts = computed(() => snapshotDaemonSummary.value?.freshness_counts || snapshotLatest.value?.freshness_counts || {});
const snapshotSourceMix = computed(() => snapshotDaemonSummary.value?.source_mix || snapshotLatest.value?.symbol_source_mix || runtimeRestBudget.value?.source_mix || {});
const snapshotWarmup = computed(() => pipelineRunControls.value?.snapshot_warmup || runtimeWarmup.value || {});
const snapshotDebugPayload = computed(() => ({
  daemon: snapshotDaemonPayload.value ? {
    status: snapshotDaemonPayload.value.status,
    daemon_status: snapshotDaemonPayload.value.daemon_status,
    watchdog_status: snapshotDaemonPayload.value.watchdog_status,
    heartbeat_age_sec: snapshotDaemonPayload.value.heartbeat_age_sec,
    current_shard_id: snapshotDaemonPayload.value.current_shard_id,
    item_count: snapshotDaemonPayload.value.item_count,
    items_returned: snapshotDaemonPayload.value.items_returned,
  } : {},
  health: snapshotDaemonSummary.value,
  rest_budget: runtimeRestBudget.value,
  latest: {
    generated_at: snapshotLatest.value?.generated_at,
    snapshot_status: snapshotLatest.value?.snapshot_status,
    freshness_counts: snapshotLatest.value?.freshness_counts,
    symbol_source_mix: snapshotLatest.value?.symbol_source_mix,
    reason_codes: snapshotLatest.value?.reason_codes,
  },
  sample_rows: snapshotRows.value.slice(0, 20),
}));
const auditStep15Snapshot = computed(() => nonEmptyObject(restHealth.value?.latest_snapshot, snapshotLatest.value));
const auditStep15Daemon = computed(() => nonEmptyObject(restHealth.value?.snapshot_daemon, snapshotDaemonSummary.value));
const auditRestBudget = computed(() => nonEmptyObject(runtimeRestBudget.value, restHealth.value?.rest_budget, auditStep15Snapshot.value));
const auditStep15Warmup = computed(() => nonEmptyObject(snapshotWarmup.value, runtimeWarmup.value));
const auditStep15Health = computed(() => {
  const latest = auditStep15Snapshot.value || {};
  const budget = auditRestBudget.value || {};
  const daemon = auditStep15Daemon.value || {};
  const warmup = auditStep15Warmup.value || {};
  const market = restHealth.value?.market_snapshot || {};
  const exchange = restHealth.value?.exchange_info || {};
  const freshnessCounts = latest.freshness_counts || daemon.freshness_counts || budget.freshness_counts || {};
  const sourceMix = latest.symbol_source_mix || daemon.source_mix || budget.source_mix || {};
  return {
    scope: latest.run_id ? "selected run evidence" : "latest runtime evidence, not selected-run evidence",
    exchange_info_source: exchange.source || latest.exchange_info_source || "unknown",
    exchange_info_cache_age_sec: exchange.cache_age_sec,
    snapshot_status: latest.snapshot_status || warmup.snapshot_status || "unknown",
    ready_status_detail: warmup.ready_status_detail || warmup.status || latest.market_snapshot_freshness_tier || "unknown",
    market_snapshot_source: market.source || latest.market_snapshot_source || "unknown",
    market_snapshot_age_sec: market.cache_age_sec ?? latest.market_snapshot_cache_age_sec,
    market_snapshot_freshness_tier: market.freshness_tier || latest.market_snapshot_freshness_tier || "unknown",
    rest_circuit_state: budget.rest_circuit_state || latest.rest_circuit_state || restHealth.value?.rest_circuit_state || "unknown",
    rest_budget_state: budget.rest_budget_state || budget.state || latest.rest_budget_state || "unknown",
    rest_recovery_stage: budget.rest_recovery_stage || latest.rest_recovery_stage || daemon.rest_recovery_stage || "unknown",
    cooldown_until: budget.cooldown_until || daemon.cooldown_until || daemon.rest_cooldown_until || restHealth.value?.rest_circuit_until || null,
    cooldown_remaining_sec: restHealth.value?.rest_circuit_remaining_sec ?? daemon.rest_circuit_remaining_sec ?? 0,
    rest_request_count: budget.rest_request_count ?? latest.rest_request_count ?? daemon.rest_request_count ?? "-",
    rest_weight_used: budget.rest_weight_used ?? latest.rest_weight_used ?? daemon.rest_weight_used ?? "-",
    rest_status_code_counts: budget.rest_status_code_counts || latest.rest_status_code_counts || daemon.rest_status_code_counts || {},
    status_418_count: budget.status_418_count ?? latest.status_418_count ?? daemon.status_418_count ?? restHealth.value?.http_418_count ?? 0,
    status_429_count: budget.status_429_count ?? latest.status_429_count ?? daemon.status_429_count ?? restHealth.value?.http_429_count ?? 0,
    live_rest_allowed: restHealth.value?.live_rest_allowed ?? true,
    candidate_allowed_count: latest.candidate_allowed_count ?? warmup.usable_symbol_count ?? "-",
    fresh_count: freshnessCounts.fresh ?? warmup.fresh_count ?? 0,
    stale_usable_count: freshnessCounts.stale_usable ?? warmup.stale_usable_count ?? 0,
    stale_blocked_count: freshnessCounts.stale_blocked ?? warmup.stale_blocked_count ?? 0,
    usable_symbol_count: warmup.usable_symbol_count ?? latest.candidate_allowed_count ?? "-",
    blocked_symbol_count: warmup.stale_blocked_count ?? latest.stale_blocked_symbol_count ?? 0,
    skipped_symbol_count: latest.skipped_symbol_count ?? 0,
    skipped_symbols: Array.isArray(latest.skipped_symbols) ? latest.skipped_symbols : [],
    reason_codes: [
      ...(Array.isArray(latest.reason_codes) ? latest.reason_codes : []),
      ...(Array.isArray(budget.reason_codes) ? budget.reason_codes : []),
      ...(Array.isArray(daemon.reason_codes) ? daemon.reason_codes : []),
    ].filter((value, index, array) => value && array.indexOf(value) === index),
    source_mix: sourceMix,
    daemon_status: daemon.daemon_status || daemon.status || warmup.daemon_status || "unknown",
    watchdog_status: daemon.watchdog_status || warmup.watchdog_status || "unknown",
    heartbeat_age_sec: daemon.heartbeat_age_sec ?? warmup.heartbeat_age_sec,
    current_shard_size: budget.current_shard_size ?? latest.current_shard_size ?? daemon.current_shard_size,
    next_shard_size: budget.next_shard_size ?? latest.next_shard_size ?? daemon.next_shard_size,
    generated_at: latest.generated_at || daemon.generated_at || budget.generated_at || restHealth.value?.generated_at || "-",
  };
});
const auditStep15SourceMixRows = computed(() => Object.entries(auditStep15Health.value.source_mix || {}));
const candidateGovernanceScopeLabel = computed(() => "Current governance snapshot, not frozen run evidence");
const candidateGovernanceGeneratedAt = computed(() => candidateGovernance.value?.generated_at || candidateGovernance.value?.source_generated_at || "-");
const candidateGovernanceUniverseAt = computed(() => candidateGovernance.value?.universe_generated_at || candidateGovernanceRows.value?.[0]?.universe_generated_at || "-");
const candidateGovernanceSnapshotAt = computed(() => candidateGovernance.value?.light_snapshot_generated_at || candidateGovernanceRows.value?.[0]?.light_snapshot_generated_at || "-");
const candidateGovernanceRows = computed(() => candidateGovernance.value?.items || []);
const candidateGovernanceCounts = computed(() => candidateGovernance.value?.counts || {});
const runAuditRuns = computed(() => runAuditList.value?.runs || []);
const currentRunIndex = computed(() => runAuditRuns.value.findIndex((row) => row.run_id === runAudit.value?.run_id));
const auditDetailsReady = computed(() =>
  Boolean(runAudit.value?.run_id) && auditSideLoadedRunId.value === runAudit.value.run_id && !auditSideLoading.value
);
const auditLazyStatusText = computed(() => {
  if (auditCoreLoading.value) return "core loading";
  if (auditSideLoading.value) return "details loading";
  if (auditLazyError.value) return "details partial";
  if (auditDetailsReady.value) return "details ready";
  if (runAudit.value?.run_id) return "details pending";
  return "audit idle";
});
const auditLazyStatusClass = computed(() => {
  if (auditLazyError.value) return "warn";
  if (auditCoreLoading.value || auditSideLoading.value) return "blue";
  if (auditDetailsReady.value) return "good";
  return "warn";
});
const previousRunId = computed(() => {
  const idx = currentRunIndex.value;
  return idx >= 0 && idx + 1 < runAuditRuns.value.length ? runAuditRuns.value[idx + 1]?.run_id : "";
});
const nextRunId = computed(() => {
  const idx = currentRunIndex.value;
  return idx > 0 ? runAuditRuns.value[idx - 1]?.run_id : "";
});
const runAuditLines = computed(() => runAudit.value?.strategy_lines || {});
const runAuditSidecarLines = computed(() => runAudit.value?.sidecar_lines || {});
const auditStrategy4Sidecar = computed(() => {
  const fromAudit = runAuditSidecarLines.value?.strategy4;
  if (fromAudit) return { ...fromAudit, evidence_scope: "selected_run_audit" };
  const paperLine = paperStats.value?.strategy4 || {};
  return {
    display_name: "异动肆号",
    strategy_line: "strategy4",
    mode: "observe_daemon",
    source_line: "without_micro",
    pipeline_selected: false,
    evidence_scope: "latest_fallback",
    daemon_state: strategy4RuntimeState.value,
    pool_count: strategy4PoolCount.value,
    status_counts: strategy4PoolCounts.value,
    attempt_count: strategy4Attempts.value?.count || strategy4AttemptRows.value.length || 0,
    attempt_executable_count: 0,
    latest_trade_plan: {
      count: strategy4PlanSummary.value.count,
      executable_count: strategy4PlanSummary.value.executable,
      output_run_id: strategy4PlanSummary.value.output_run_id,
      output_fresh: !strategy4PlanSummary.value.stale,
      stale_output_reason: strategy4PlanSummary.value.stale_reason || ""
    },
    downstream: {
      paper_orders: paperLine.total_orders || 0,
      paper_closed: paperLine.closed_orders || 0,
      paper_skips: 0,
      trade_quality_samples: 0
    },
    reason_codes: []
  };
});
const auditStrategy4SidecarFallback = computed(() => !runAuditSidecarLines.value?.strategy4);
const auditStrategy4StatusCounts = computed(() => auditStrategy4Sidecar.value?.status_counts || {});
const auditStrategy4LatestPlan = computed(() => auditStrategy4Sidecar.value?.latest_trade_plan || {});
const auditStrategy4Downstream = computed(() => auditStrategy4Sidecar.value?.downstream || {});
const runAuditSymbols = computed(() => {
  const rows = runAudit.value?.symbols || [];
  if (auditLineFilter.value === "all") return rows;
  return rows.filter((row) => row.strategy_line === auditLineFilter.value);
});
const runAuditStatusClass = computed(() => statusClass(runAudit.value?.status));
const microQualityRows = computed(() => {
  const rows = microQualityAudit.value?.symbols || [];
  if (auditLineFilter.value === "all") return rows;
  return rows.filter((row) => row.strategy_line === auditLineFilter.value || row.line === auditLineFilter.value);
});
const microQualitySummary = computed(() => microQualityAudit.value?.summary || {});
const microEvidenceRows = computed(() => {
  const rows = microEvidenceRuntime.value?.symbols || [];
  if (auditLineFilter.value === "all") return rows;
  return rows.filter((row) => row.strategy_line === auditLineFilter.value || row.line === auditLineFilter.value);
});
const microEvidenceSummary = computed(() => microEvidenceRuntime.value?.summary || {});
const microTrainingRunRows = computed(() => microTrainingLatest.value?.run_samples || []);
const microTrainingSymbolRows = computed(() => (microTrainingLatest.value?.symbols || []).slice(0, 120));
const microTrainingHistoryRows = computed(() => microTrainingRuns.value?.runs || []);
const microTrainingSummary = computed(() => microTrainingLatest.value || {});
const microTrainingCoverageRatio = computed(() => microTrainingCoverage.value?.run_coverage_ratio ?? microTrainingSummary.value?.run_coverage_ratio);
const microTrainingMetricCoverage = computed(() => microTrainingSummary.value?.run_metric_coverage || microTrainingSummary.value?.metric_coverage || {});
const microTargetDistributionRows = computed(() => {
  const dist = microEvidenceTargetSource.value?.target_source_distribution || {};
  return Object.entries(dist)
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => Number(b.count) - Number(a.count));
});
const microTargetRows = computed(() => microEvidenceTargetSource.value?.targets || []);
const microFullZRows = computed(() => {
  const rows = microFullZAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_full") return rows;
  return [];
});
const microFastRuntimeRows = computed(() => {
  const rows = microFastRuntimeAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microFastTailCleanupRows = computed(() => {
  const rows = microFastTailCleanupAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microFastJudgeableRows = computed(() => {
  const rows = microFastJudgeableAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microFastJudgeableOnlyRows = computed(() => {
  const rows = microFastJudgeableOnlyAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microFastJudgeableThroughputRows = computed(() => {
  const rows = microFastJudgeableThroughputAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microFastCoverageSplitRows = computed(() => {
  const rows = microFastCoverageSplitAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microFastValidBucketRows = computed(() => {
  const rows = microFastValidBucketAudit.value?.symbols || [];
  if (auditLineFilter.value === "all" || auditLineFilter.value === "micro_fast") return rows;
  return [];
});
const microEvidenceReasonFunnel = computed(() => {
  const counts = {};
  for (const row of microEvidenceRows.value) {
    for (const reason of row.raw_reasons || []) {
      if (!counts[reason]) counts[reason] = { reason, count: 0, p0: 0, symbols: new Set(), attributed: new Set() };
      counts[reason].count += 1;
      if (row.severity === "P0") counts[reason].p0 += 1;
      if (row.symbol) counts[reason].symbols.add(row.symbol);
      for (const attr of row.attributed_reasons || []) counts[reason].attributed.add(attr);
    }
  }
  return Object.values(counts)
    .map((row) => ({
      ...row,
      symbols: row.symbols.size,
      attributed: Array.from(row.attributed).slice(0, 3).join(", ")
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 12);
});
const microQualityFreshness = computed(() => {
  const source = String(microQualityAudit.value?.source || "");
  if (source === "missing_current_run") return "missing";
  const auditRunId = runAudit.value?.run_id;
  const qualityRunId = microQualityAudit.value?.run_id;
  if (!auditRunId || !qualityRunId) return "missing";
  return String(auditRunId) === String(qualityRunId) ? "fresh" : "stale";
});

const microEvidenceFreshness = computed(() => {
  const auditRunId = runAudit.value?.run_id;
  const evidenceRunId = microEvidenceRuntime.value?.run_id;
  if (!auditRunId || !evidenceRunId) return "missing";
  return String(auditRunId) === String(evidenceRunId) ? "fresh" : "stale";
});

function microQualityCount(key) {
  return microQualitySummary.value?.category_counts?.[key] || microQualitySummary.value?.[`${key}_count`] || 0;
}

function microQualityLine(row) {
  return row.strategy_line || row.line || "-";
}

function microQualityRawReason(row) {
  if (row.raw_reason) return row.raw_reason;
  if (Array.isArray(row.raw_reasons)) return row.raw_reasons.join(", ");
  return "-";
}

function microQualityAttribution(row) {
  return row.attributed_reason || row.attributions?.[0]?.attributed_reason || "-";
}

function microQualityCategory(row) {
  return row.category || row.attributions?.[0]?.category || "-";
}

function microQualityAction(row) {
  return row.recommended_action || row.attributions?.[0]?.recommended_action || "-";
}

function microQualityEvidence(row, key) {
  const value = row.evidence?.[key];
  if (value === null || value === undefined || value === "") return "-";
  return value;
}

function microQualityDriverMetrics(row) {
  const metrics = row.evidence?.driver_metrics_summary || {};
  const parts = [
    `cvd ${metrics.cvd_update_count ?? "-"}`,
    `ofi ${metrics.ofi_update_count ?? "-"}`,
    `bucket ${metrics.processed_bucket_count ?? "-"}`
  ];
  return parts.join(" / ");
}

function microEvidenceCount(key) {
  const counts = microEvidenceSummary.value?.severity_counts || microEvidenceSummary.value?.status_counts || {};
  return counts[key] || microEvidenceSummary.value?.[`${String(key).toLowerCase()}_count`] || 0;
}

function microEvidenceFrame(row, key) {
  const value = row.factor_frame?.[key] ?? row.runtime_evidence?.[key] ?? row.z_window?.[key];
  if (value === null || value === undefined || value === "") return "-";
  return value;
}

function microEvidenceRuntimeValue(row, path) {
  const parts = String(path || "").split(".");
  let value = row;
  for (const part of parts) {
    if (value === null || value === undefined) return "-";
    value = value[part];
  }
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "boolean") return value ? "yes" : "no";
  return value;
}

function microEvidenceCoverage(row, stream) {
  const entry = row.runtime_evidence?.coverage?.[stream] || row.stream_heartbeat?.streams?.[stream] || {};
  const ratio = entry.coverage_ratio;
  const cls = entry.root_cause || entry.gap_class || entry.missing_reason || "ok";
  if (ratio === null || ratio === undefined) return cls;
  return `${Math.round(Number(ratio) * 100)}% · ${cls}`;
}

function microEvidenceStoreWindow(row, key) {
  const store = row.z_window?.store_window || row.runtime_evidence?.z_history_runtime?.store_window || {};
  const value = store[key];
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number" && !Number.isInteger(value)) return Number(value).toFixed(3);
  return value;
}

function formatPct(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${Math.round(num * 100)}%`;
}

function statusClass(value) {
  const text = String(value || "").toLowerCase();
  if (["ok", "healthy", "running", "fresh", "success", "mock_sent", "pass", "closed"].some((x) => text.includes(x))) return "good";
  if (["warn", "stale", "stopped", "disabled", "waiting", "degraded", "partial", "half_open"].some((x) => text.includes(x))) return "warn";
  if (["failed", "error", "bad", "blocked", "open", "fail_closed"].some((x) => text.includes(x))) return "bad";
  return "blue";
}

function ageLabel(iso) {
  if (!iso) return "-";
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (!Number.isFinite(seconds)) return "-";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${Math.round(seconds / 3600)}h`;
}

function freshnessStatus(iso) {
  if (!iso) return "missing";
  const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (seconds < 180) return "fresh";
  if (seconds < 1500) return "stale";
  return "old";
}

function money(value) {
  const num = Number(value || 0);
  return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return num.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function bytesLabel(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = num;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  return `${size.toLocaleString(undefined, { maximumFractionDigits: idx === 0 ? 0 : 2 })} ${units[idx]}`;
}

function microTrainingValue(row, key) {
  const value = row?.[key];
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number" && !Number.isInteger(value)) return value.toFixed(4);
  return value;
}

function microCoverageMetric(key) {
  const item = microTrainingMetricCoverage.value?.[key] || {};
  return formatPct(item.coverage);
}

function signedR(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${num >= 0 ? "+" : ""}${num.toFixed(3)}R`;
}

function ratioX(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${num.toFixed(2)}x`;
}

function minutesLabel(value) {
  if (value === null || value === undefined || value === "") return "-";
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${num.toFixed(num >= 10 ? 0 : 1)}m`;
}

function losingStreakLabel(stats) {
  const maxStreak = Number(stats?.max_losing_streak || 0);
  const distribution = stats?.losing_streak_distribution || {};
  const parts = Object.entries(distribution)
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([length, count]) => `${length}x:${count}`);
  if (!maxStreak && !parts.length) return "-";
  return `max ${maxStreak}${parts.length ? ` / ${parts.join(", ")}` : ""}`;
}

function shortJson(value, maxLength = 600) {
  const text = JSON.stringify(value || {}, null, 2);
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}...`;
}

function currentRunRows(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (currentRunIds.value.size === 0) return list;
  return list.filter((row) => currentRunIds.value.has(row.source_run_id));
}

function rowsForLine(source, line) {
  const data = source || {};
  if (line === "overview") return Object.values(data).flat();
  return data[line] || [];
}

function sortByTime(rows, keys) {
  return [...(Array.isArray(rows) ? rows : [])].sort((a, b) => {
    const av = firstTime(a, keys);
    const bv = firstTime(b, keys);
    return bv - av;
  });
}

function firstTime(row, keys) {
  for (const key of keys) {
    const value = row?.[key];
    if (!value) continue;
    const time = new Date(value).getTime();
    if (Number.isFinite(time)) return time;
  }
  return 0;
}

function marketOrdersForLine(line) {
  return rowsForLine(paper.value?.orders, line).filter(isMarketExecutableOrder);
}

function positionsForLine(line) {
  return rowsForLine(paper.value?.positions, line);
}

function skippedForLine(line) {
  const rows = paper.value?.skipped_signals || [];
  const filtered = line === "overview" ? rows : rows.filter((row) => row.strategy_line === line || row.line === line);
  return currentRunRows(filtered);
}

function isMarketExecutableOrder(row) {
  return Number(row?.source_executable || 0) === 1
    && String(row?.order_type || "").toLowerCase() === "market"
    && String(row?.source_action || "").toUpperCase() === "ENTER_MARKET"
    && String(row?.source_entry_mode || "").toUpperCase() === "MARKET";
}

function price(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num === 0) return "-";
  return num.toLocaleString(undefined, { maximumFractionDigits: 8 });
}

function quantity(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num) || num === 0) return "-";
  return num.toLocaleString(undefined, { maximumFractionDigits: 6 });
}

function lineLabel(line) {
  return lineNames[line] || line || "-";
}

function lineSelectedLabel(line) {
  const explicit = runAuditLines.value?.[line]?.selected;
  if (explicit === false) return "skipped";
  if (explicit === true) return "selected";
  const selected = runAudit.value?.summary?.line_selected?.[line];
  if (selected === false) return "skipped";
  if (selected === true) return "selected";
  return "unknown";
}

function lineSymbolsCount(line) {
  const symbols = runAuditLines.value?.[line]?.symbols;
  if (Array.isArray(symbols)) return symbols.length;
  return runAudit.value?.summary?.line_symbol_count?.[line] || 0;
}

function lineActionText(line) {
  const dist = runAuditLines.value?.[line]?.action_distribution || {};
  const parts = Object.entries(dist).map(([key, value]) => `${key}:${value}`);
  return parts.join(" / ") || "-";
}

function lineStageText(line) {
  const stage = runAuditLines.value?.[line]?.stage_status || {};
  const failed = Array.isArray(stage.failed_stages) ? stage.failed_stages.length : 0;
  return `${stage.stage_count ?? "-"} stages / ${failed} failed`;
}

function auditStepDetailText(step) {
  const detail = step?.detail;
  if (detail === null || detail === undefined || detail === "") return "";
  if (typeof detail === "string" || typeof detail === "number" || typeof detail === "boolean") return String(detail);
  if (detail.path) return detail.generated_at ? `${detail.generated_at} / ${detail.path}` : detail.path;
  if (detail.stage_count !== undefined) return `${detail.stage_count} stages / failed ${(detail.failed_stages || []).length}`;
  if (detail.wait_detail) return JSON.stringify(detail.wait_detail).slice(0, 180);
  return JSON.stringify(detail).slice(0, 180);
}

function sideLabel(side) {
  return String(side || "").toUpperCase() === "SHORT" ? "空" : "多";
}

function orderTypeLabel(type) {
  const got = String(type || "").toLowerCase();
  if (got === "market") return "市价";
  if (got === "limit") return "限价";
  if (got === "trigger") return "触发";
  return got || "-";
}

function exitReasonLabel(reason) {
  const got = String(reason || "").toUpperCase();
  if (got === "TP") return "止盈";
  if (got === "SL") return "止损";
  if (got === "MANUAL") return "手动";
  return got || "-";
}

function skipReasonLabel(reason) {
  const map = {
    non_executable: "计划不可执行",
    pending_not_allowed: "非市价，已跳过",
    missing_price_contract: "价格契约缺失",
    source_plan_hash_consumed: "信号已处理",
    active_slot_occupied: "同策略同交易对已有仓位",
    non_entry_decision: "不是开仓方向"
  };
  return map[reason] || reason || "-";
}

function pnlClass(value) {
  const num = Number(value || 0);
  if (num > 0) return "up";
  if (num < 0) return "down";
  return "flat";
}

function viewStatsForLine(line = paperTab.value) {
  const stats = line === "overview" ? paper.value?.stats?.global || {} : paperStats.value?.[line] || {};
  return {
    ...stats,
    pass_count: skippedForLine(line).length,
    scope: "All Ledger"
  };
}

function healthText(health) {
  const status = health?.daemon_status || health?.status || "unknown";
  const age = health?.heartbeat_age_sec;
  return age === null || age === undefined ? status : `${status} · ${age}s heartbeat`;
}

function isRunningHealth(health) {
  return String(health?.daemon_status || health?.status || "").toLowerCase() === "running" && health?.stale !== true;
}

function secondsText(value) {
  if (value === null || value === undefined || value === "") return "-";
  const n = Number(value);
  if (!Number.isFinite(n)) return String(value);
  if (n < 60) return `${Math.max(0, Math.round(n))}s`;
  const minutes = Math.floor(n / 60);
  const seconds = Math.round(n % 60);
  return seconds ? `${minutes}m ${seconds}s` : `${minutes}m`;
}

function log(message) {
  runtimeLog.value = [`${new Date().toLocaleTimeString()} ${message}`, ...runtimeLog.value].slice(0, 8);
}

function pipelineLog(message, type = "ok") {
  const stamp = new Date().toLocaleTimeString("zh-CN", { hour12: false });
  pipelineUi.value.logs = [{ text: `${stamp} ${message}`, type }, ...pipelineUi.value.logs].slice(0, 60);
}

function configPretty(value) {
  return JSON.stringify(value || {}, null, 2);
}

function configValueByPath(source, path) {
  return path.reduce((obj, key) => (obj && obj[key] !== undefined ? obj[key] : {}), source || {});
}

function syncConfigDrafts(snapshot = config.value) {
  const next = {};
  for (const line of configLineSections) {
    next[`line:${line}`] = configPretty(snapshot?.trade_plan_lines?.[line]);
  }
  for (const section of configSections) {
    next[`section:${section.key}`] = configPretty(configValueByPath(snapshot, section.path));
  }
  configDrafts.value = next;
}

function parseConfigDraft(key) {
  try {
    return JSON.parse(configDrafts.value[key] || "{}");
  } catch (exc) {
    throw new Error(`Config JSON invalid: ${exc.message}`);
  }
}

function configDraftObject(key) {
  try {
    return JSON.parse(configDrafts.value[key] || "{}");
  } catch {
    return {};
  }
}

function configDraftField(key, field, fallback = "") {
  const obj = configDraftObject(key);
  return obj[field] ?? fallback;
}

function updateConfigDraftField(key, field, value) {
  const obj = configDraftObject(key);
  obj[field] = value;
  configDrafts.value = {
    ...configDrafts.value,
    [key]: JSON.stringify(obj, null, 2)
  };
}

function updateConfigDraftBool(key, field, event) {
  updateConfigDraftField(key, field, Boolean(event?.target?.checked));
}

function configDraftNestedField(key, path, fallback = "") {
  const obj = configDraftObject(key);
  let cur = obj;
  for (const part of path) {
    if (!cur || cur[part] === undefined) return fallback;
    cur = cur[part];
  }
  return cur ?? fallback;
}

function updateConfigDraftNestedField(key, path, value) {
  const obj = configDraftObject(key);
  let cur = obj;
  for (const part of path.slice(0, -1)) {
    if (!cur[part] || typeof cur[part] !== "object" || Array.isArray(cur[part])) cur[part] = {};
    cur = cur[part];
  }
  cur[path[path.length - 1]] = value;
  configDrafts.value = {
    ...configDrafts.value,
    [key]: JSON.stringify(obj, null, 2)
  };
}

function updateConfigDraftNestedNumber(key, path, event) {
  const raw = event?.target?.value;
  const num = raw === "" ? null : Number(raw);
  updateConfigDraftNestedField(key, path, Number.isFinite(num) ? num : raw);
}

function updateConfigDraftNestedBool(key, path, event) {
  updateConfigDraftNestedField(key, path, Boolean(event?.target?.checked));
}

function configImpactGroup(key) {
  return configUiSchema.value?.groups?.[key]?.fields || [];
}

function configImpactCount(key) {
  return configUiSchema.value?.groups?.[key]?.count ?? configImpactGroup(key).length;
}

function configEffectiveForLine(line) {
  return configEffective.value?.[line] || {};
}

function configEffectiveCounts(line) {
  return configEffectiveForLine(line)?.counts || {};
}

function configLineWarning(line) {
  const notes = configEffectiveForLine(line)?.notes || [];
  return notes[0] || "";
}

function lineImpactRows(line, options = {}) {
  const effective = configEffectiveForLine(line) || {};
  let rows = effective.fields || [];
  if (options.recommendation === "hide_legacy") rows = effective.legacy_fields || [];
  if (options.recommendation === "primary") rows = effective.direct_executable_fields || [];
  const recommendation = options.recommendation;
  const contains = options.contains || [];
  const stage = options.stage || "";
  return rows.filter((row) => {
    if (recommendation && row.ui_recommendation !== recommendation) return false;
    if (stage && !(row.business_stages || []).includes(stage)) return false;
    if (contains.length && !contains.some((token) => String(row.field_path || "").includes(token))) return false;
    return true;
  });
}

function topLineImpactRows(line, options = {}, limit = 6) {
  return lineImpactRows(line, options).slice(0, limit);
}

function configImpactTagClass(row) {
  if (row?.status === "legacy" || row?.status === "disabled" || row?.ui_recommendation === "hide_legacy") return "warn";
  if (row?.direct_executable_impact) return "good";
  if (row?.paper_impact) return "blue";
  return "blue";
}

function configImpactLabel(row) {
  if (row?.status === "legacy" || row?.status === "disabled") return row.status;
  if (row?.direct_executable_impact) return "executable";
  if (row?.paper_impact) return "paper";
  if (row?.backtest_impact) return "backtest";
  return row?.ui_recommendation || "field";
}

async function saveStrategyLineConfig(line) {
  const key = `line:${line}`;
  configSaving.value = key;
  configMessage.value = "";
  try {
    const values = { [line]: parseConfigDraft(key) };
    await api.validateConfig("trade-plan-lines", values);
    await api.updateConfig("trade-plan-lines", values);
    configMessage.value = `${line} saved`;
    await refreshAll();
  } catch (exc) {
    configMessage.value = exc.message;
  } finally {
    configSaving.value = "";
  }
}

async function saveConfigSection(section) {
  const key = `section:${section.key}`;
  configSaving.value = key;
  configMessage.value = "";
  try {
    const values = parseConfigDraft(key);
    await api.validateConfig(section.key, values);
    await api.updateConfig(section.key, values);
    configMessage.value = `${section.title} saved`;
    await refreshAll();
  } catch (exc) {
    configMessage.value = exc.message;
  } finally {
    configSaving.value = "";
  }
}

async function applyConfigProfile(profileName) {
  configSaving.value = `profile:${profileName}`;
  configMessage.value = "";
  try {
    const result = await api.applyConfigProfile(profileName);
    configMessage.value = `profile applied: ${result.active_profile || profileName}`;
    await refreshAll();
  } catch (exc) {
    configMessage.value = exc.message;
  } finally {
    configSaving.value = "";
  }
}

async function reloadRuntimeConfig() {
  configSaving.value = "reload";
  configMessage.value = "";
  try {
    await api.reloadConfig();
    configMessage.value = "config reloaded";
    await refreshAll();
  } catch (exc) {
    configMessage.value = exc.message;
  } finally {
    configSaving.value = "";
  }
}

function stageIndexByProgress(percent) {
  if (percent <= 0) return -1;
  if (percent >= 100) return pipelineStages.length - 1;
  return Math.min(pipelineStages.length - 1, Math.floor(percent / (100 / pipelineStages.length)));
}

function stageState(line, index) {
  const state = pipelineLineState(line);
  if (state === "skipped") {
    return {
      done: false,
      active: false,
      skipped: true
    };
  }
  if (state === "blocked") {
    return {
      done: index < 2,
      active: false,
      blocked: index === 2
    };
  }
  if (state === "failed") {
    return {
      done: false,
      active: index === Math.max(0, stageIndexByProgress(Number(pipelineUi.value.lines[line]?.percent || 0))),
      failed: index === Math.max(0, stageIndexByProgress(Number(pipelineUi.value.lines[line]?.percent || 0)))
    };
  }
  const percent = Number(pipelineUi.value.lines[line]?.percent || 0);
  const threshold = ((index + 1) / pipelineStages.length) * 100;
  const active = stageIndexByProgress(percent) === index && percent > 0 && percent < threshold;
  return {
    done: percent >= threshold,
    active
  };
}

function pipelineFunnelLine(line) {
  return pipelineFunnel.value?.lines?.[line] || {};
}

function pipelineFunnelStages(line) {
  const stages = pipelineFunnelLine(line)?.stage_cards;
  if (Array.isArray(stages) && stages.length) return stages;
  return pipelineStages.map((stage) => ({
    ...stage,
    status: "unknown",
    counts: {},
    reason_codes: [],
    breakpoint: false
  }));
}

function funnelStageClass(stage) {
  const status = String(stage?.status || "unknown").toLowerCase();
  return {
    "funnel-ok": status === "ok",
    "funnel-empty": status === "empty",
    "funnel-blocked": status === "blocked",
    "funnel-stale": status === "stale",
    "funnel-na": status === "not_applicable",
    "funnel-breakpoint": Boolean(stage?.breakpoint)
  };
}

function funnelStageSubtitle(stage) {
  const counts = stage?.counts || {};
  const parts = Object.entries(counts)
    .filter(([, value]) => value !== undefined && value !== null && value !== "")
    .slice(0, 3)
    .map(([key, value]) => `${key}:${value}`);
  if (parts.length) return parts.join(" ");
  return stage?.status || "-";
}

function funnelStageTitle(stage) {
  const counts = stage?.counts || {};
  const reasons = Array.isArray(stage?.reason_codes) ? stage.reason_codes : [];
  const countText = Object.entries(counts).map(([key, value]) => `${key}: ${value}`).join("\n");
  const reasonText = reasons.length ? `\nreasons: ${reasons.join(", ")}` : "";
  return `${stage?.code || ""} ${stage?.name || ""}\nstatus: ${stage?.status || "-"}${countText ? `\n${countText}` : ""}${reasonText}`;
}

function funnelBreakpointLabel(line) {
  const row = pipelineFunnelLine(line);
  const stage = row?.breakpoint_stage || "-";
  const reasons = Array.isArray(row?.breakpoint_reason_codes) ? row.breakpoint_reason_codes : [];
  return reasons.length ? `${stage} · ${reasons.slice(0, 2).join(", ")}` : stage;
}

function pipelineLineState(line) {
  const row = pipelineUi.value.lines[line] || {};
  const stage = String(row.stage || "").toLowerCase();
  const terminalState = String(row.terminal_state || "").toLowerCase();
  const stageClass = String(row.stage_status_class || "").toLowerCase();
  if (row.skipped || stage === "skipped_not_selected") return "skipped";
  if (stageClass === "business_no_signal") return "no_signal";
  if (stageClass === "business_partial_consumable") return "partial";
  if (stageClass === "completed_with_consumable") return "completed";
  if (stageClass === "technical_failed") return "failed";
  if (terminalState === "blocked") return "blocked";
  if (terminalState === "failed") return "failed";
  if (terminalState === "skipped") return "skipped";
  if (stage.startsWith("blocked_")) return "blocked";
  if (
    stage === "completed_with_unfinished_symbols"
    || (row.line_lifecycle_status === "partial_ready" && Number(row.unfinished_symbol_count || 0) > 0)
    || row.line_lifecycle_status === "observing"
  ) return "partial";
  if (stage.includes("degraded") || row.status === "degraded") return "degraded";
  if (stage.includes("failed") || row.status === "failed") return "failed";
  if (stage === "completed" || row.done) return "completed";
  if (["once", "cycle"].includes(pipelineUi.value.mode) && Number(row.percent || 0) > 0) return "running";
  return "waiting";
}

function pipelineLineStateLabel(line) {
  const state = pipelineLineState(line);
  const labels = {
    blocked: "Blocked",
    partial: "Partial",
    degraded: "Degraded",
    failed: "Failed",
    no_signal: "No Signal",
    completed: "Completed",
    skipped: "Skipped",
    running: "Running",
    waiting: "Waiting"
  };
  return labels[state] || state;
}

function pipelineLineStateClass(line) {
  const state = pipelineLineState(line);
  return {
    blocked: "warn",
    partial: "warn",
    degraded: "warn",
    failed: "bad",
    no_signal: "blue",
    completed: "good",
    skipped: "blue",
    running: "blue",
    waiting: "blue"
  }[state] || "blue";
}

function pipelineLineReason(line) {
  const row = pipelineUi.value.lines[line] || {};
  const stage = String(row.stage || "");
  const stageClass = String(row.stage_status_class || "");
  if (stageClass === "business_no_signal") return `business terminal · ${String(row.business_terminal_reason || "no consumable signal").replaceAll("_", " ")}`;
  if (stageClass === "technical_failed") return String(row.technical_failure_reason || "technical failure").replaceAll("_", " ");
  if (stageClass === "completed_with_consumable") return "consumable symbols ready";
  if (stageClass === "business_partial_consumable") return "partial consumable symbols";
  if (row.terminal_reason) return String(row.terminal_reason).replaceAll("_", " ");
  if (pipelineLineState(line) === "skipped") return "not selected for this run";
  if (stage.startsWith("blocked_micro_unhealthy")) return "micro daemon unhealthy";
  if (stage.startsWith("blocked_micro_full_wait_timeout")) return "full warmup incomplete";
  if (stage.startsWith("blocked_")) return stage.replace(/^blocked_/, "").replaceAll("_", " ");
  if (pipelineLineState(line) === "partial") {
    const left = Number(row.unfinished_symbol_count || 0);
    return left > 0 ? `partial ready · ${left} symbols still unfinished` : "partial ready";
  }
  if (String(row.line_lifecycle_status || "").startsWith("terminalized_")) {
    const counts = row.symbol_counts || {};
    const consumable = Number(row.consumable_symbol_count ?? counts.consumable ?? 0);
    const rejected = Number(row.rejected_count ?? counts.rejected ?? 0);
    const notReady = Number(row.not_ready_count ?? counts.not_ready ?? 0);
    const timeout = Number(row.timeout_count ?? counts.timeout ?? 0);
    return `${consumable} consumable 路 ${rejected} rejected 路 ${notReady} not ready 路 ${timeout} timeout`;
  }
  if (pipelineLineState(line) === "completed") return "normal completion";
  return stage || "waiting";
}

function setPipelineFromApi(payload) {
  const progress = payload?.progress || {};
  const lines = progress.lines || {};
  const controls = payload?.run_controls || {};
  const running = Boolean(payload?.job_running || payload?.active_job || controls.can_stop === true);
  const waiting = payload?.display_state === "interval_waiting" || payload?.active_interval?.status === "interval_waiting";
  pipelineUi.value.mode = running
    ? payload?.active_job?.mode === "interval"
      ? "cycle"
      : "once"
    : waiting
      ? "cycle_waiting"
    : progress.status === "ok"
      ? "done"
      : "idle";
  pipelineUi.value.overall = Number(progress.overall_percent || 0);
  for (const line of pipelineStrategyLineOrder) {
    pipelineUi.value.lines[line] = {
      percent: Number(lines[line]?.percent || 0),
      stage: lines[line]?.stage || "waiting",
      status: lines[line]?.status || null,
      done: Boolean(lines[line]?.done),
      selected: lines[line]?.selected !== false,
      skipped: Boolean(lines[line]?.skipped),
      run_id: lines[line]?.run_id || lines[line]?.output_run_id || null,
      output_fresh: Boolean(lines[line]?.output_fresh),
      output_generated_at: lines[line]?.output_generated_at || null,
      terminal_state: lines[line]?.terminal_state || null,
      terminal_reason: lines[line]?.terminal_reason || null,
      stage_status_class: lines[line]?.stage_status_class || null,
      business_terminal_reason: lines[line]?.business_terminal_reason || null,
      technical_failure_reason: lines[line]?.technical_failure_reason || null,
      technical_blocked: Boolean(lines[line]?.technical_blocked),
      technical_block_reason: lines[line]?.technical_block_reason || null,
      recovery: lines[line]?.recovery || null,
      line_exec_status: lines[line]?.line_exec_status || null,
      line_lifecycle_status: lines[line]?.line_lifecycle_status || null,
      wait_result: lines[line]?.wait_result || null,
      terminalized_symbol_count: Number(lines[line]?.terminalized_symbol_count || 0),
      unfinished_symbol_count: Number(lines[line]?.unfinished_symbol_count || 0),
      consumable_symbol_count: Number(lines[line]?.consumable_symbol_count || 0),
      rejected_count: Number(lines[line]?.rejected_count || 0),
      not_ready_count: Number(lines[line]?.not_ready_count || 0),
      timeout_count: Number(lines[line]?.timeout_count || 0),
      observing_count: Number(lines[line]?.observing_count || 0),
      symbol_counts: lines[line]?.symbol_counts || null
    };
  }
  const activeSelected = payload?.selected_lines || progress?.selected_lines;
  if ((running || waiting) && Array.isArray(activeSelected) && activeSelected.length) {
    selectedPipelineLines.value = activeSelected;
  }
}

function togglePipelineLine(line) {
  if (isPipelineRunning.value || loading.value) return;
  const current = new Set(selectedPipelineLines.value);
  if (current.has(line)) {
    if (current.size <= 1) return;
    current.delete(line);
  } else {
    current.add(line);
  }
  selectedPipelineLines.value = ["without_micro", "micro_fast", "micro_full"].filter((item) => current.has(item));
}

function animatePipelineProgress() {
  // STEP13.14: progress is API-owned. The UI may show a running stripe,
  // but it must not invent deterministic percentages.
}

function startPipelinePolling() {
  if (pipelinePollTimer.value) {
    clearInterval(pipelinePollTimer.value);
  }
  pipelinePollTimer.value = setInterval(async () => {
    if (pipelinePollInFlight.value) return;
    pipelinePollInFlight.value = true;
    try {
      const status = await api.pipelineStatusLite();
      const watchdog = await api.pipelineWatchdog().catch(() => null);
      const funnel = await api.pipelineFunnelLatest(true).catch(() => null);
      pipeline.value = status;
      if (watchdog) pipelineWatchdog.value = watchdog;
      if (funnel) pipelineFunnel.value = funnel;
      const wasRunning = ["once", "cycle", "cycle_waiting"].includes(pipelineUi.value.mode);
      setPipelineFromApi(status);
      const waiting = status?.display_state === "interval_waiting" || status?.active_interval?.status === "interval_waiting";
      if (waiting) {
        pipelineUi.value.mode = "cycle_waiting";
      }
      const activeByControl = Boolean(status?.run_controls?.can_stop === true || status?.job_running || status?.active_job);
      if (!activeByControl && !waiting && wasRunning) {
        const latestStatus = status.latest_report?.status || status.latest_report_summary?.status;
        pipelineLog("pipeline finished or stopped", latestStatus === "failed" ? "warn" : "ok");
        pipelineUi.value.mode = latestStatus === "ok" ? "done" : "idle";
        clearInterval(pipelinePollTimer.value);
        pipelinePollTimer.value = null;
        await refreshAll();
      }
    } catch (exc) {
      pipelineLog(`status poll failed: ${exc.message}`, "warn");
    } finally {
      pipelinePollInFlight.value = false;
    }
  }, 1500);
}

function scheduleIdle(fn) {
  if (typeof window !== "undefined" && typeof window.requestIdleCallback === "function") {
    window.requestIdleCallback(fn, { timeout: 1200 });
    return;
  }
  setTimeout(fn, 0);
}

function clearAuditSidePanels() {
  auditLazySeq.value += 1;
  auditSideLoading.value = false;
  auditDetailsRequested.value = false;
  showAuditRawPayload.value = false;
  microQualityAudit.value = { symbols: [], summary: {} };
  microEvidenceRuntime.value = { symbols: [], summary: {} };
  microEvidenceTargetSource.value = { targets: [], target_source_distribution: {} };
  microFullZAudit.value = { symbols: [], summary: {} };
  microFastRuntimeAudit.value = { symbols: [], summary: {} };
  microFastTailCleanupAudit.value = { symbols: [], summary: {} };
  microFastJudgeableAudit.value = { symbols: [], summary: {} };
  microFastJudgeableOnlyAudit.value = { symbols: [], summary: {} };
  microFastJudgeableThroughputAudit.value = { symbols: [], summary: {} };
  microFastCoverageSplitAudit.value = { symbols: [], summary: {} };
  microFastValidBucketAudit.value = { symbols: [], summary: {} };
  candidateGovernance.value = { items: [], counts: {} };
  auditSideLoadedRunId.value = "";
}

async function loadAuditCore(runId = "") {
  auditCoreLoading.value = true;
  auditLazyError.value = "";
  try {
    const results = await Promise.allSettled([
      runId ? api.runAuditById(runId) : api.runAuditLatestLite(),
      api.runAuditsLite(12),
      api.paperConsumptionStatus(runId || undefined)
    ]);
    const [auditPayload, listPayload, paperPayload] = results;
    if (auditPayload.status !== "fulfilled") throw auditPayload.reason || new Error("run audit missing");
    const nextRunId = auditPayload.value?.run_id || runId;
    if (nextRunId !== runAudit.value?.run_id) {
      clearAuditSidePanels();
    }
    runAudit.value = auditPayload.value;
    if (listPayload.status === "fulfilled") runAuditList.value = listPayload.value;
    if (paperPayload.status === "fulfilled") paperConsumption.value = paperPayload.value;
    auditLineFilter.value = "all";
    return nextRunId;
  } finally {
    auditCoreLoading.value = false;
  }
}

async function loadAuditSidePanels(runId = "") {
  const targetRunId = runId || runAudit.value?.run_id || "";
  if (!targetRunId || auditSideLoading.value) return;
  const seq = auditLazySeq.value + 1;
  auditLazySeq.value = seq;
  auditSideLoading.value = true;
  auditLazyError.value = "";
  try {
    const results = await Promise.allSettled([
      api.microQualityById(targetRunId),
      api.microEvidenceById(targetRunId),
      api.microEvidenceTargetSource(),
      api.microFullZById(targetRunId),
      api.microFastRuntimeById(targetRunId),
      api.microFastTailCleanupById(targetRunId),
      api.microFastJudgeableById(targetRunId),
      api.microFastJudgeableOnlyById(targetRunId),
      api.microFastJudgeableThroughputById(targetRunId),
      api.microFastCoverageSplitById(targetRunId),
      api.microFastValidBucketById(targetRunId),
      api.candidatePoolGovernance(120)
    ]);
    if (seq !== auditLazySeq.value || targetRunId !== runAudit.value?.run_id) return;
    const [mq, me, mts, mfz, mfr, mft, mfj, mfjo, mfjt, mfcs, mfvb, cg] = results;
    if (mq.status === "fulfilled") microQualityAudit.value = mq.value;
    if (me.status === "fulfilled") microEvidenceRuntime.value = me.value;
    if (mts.status === "fulfilled") microEvidenceTargetSource.value = mts.value;
    if (mfz.status === "fulfilled") microFullZAudit.value = mfz.value;
    if (mfr.status === "fulfilled") microFastRuntimeAudit.value = mfr.value;
    if (mft.status === "fulfilled") microFastTailCleanupAudit.value = mft.value;
    if (mfj.status === "fulfilled") microFastJudgeableAudit.value = mfj.value;
    if (mfjo.status === "fulfilled") microFastJudgeableOnlyAudit.value = mfjo.value;
    if (mfjt.status === "fulfilled") microFastJudgeableThroughputAudit.value = mfjt.value;
    if (mfcs.status === "fulfilled") microFastCoverageSplitAudit.value = mfcs.value;
    if (mfvb.status === "fulfilled") microFastValidBucketAudit.value = mfvb.value;
    if (cg.status === "fulfilled") candidateGovernance.value = cg.value;
    const failed = results.filter((item) => item.status === "rejected").length;
    auditSideLoadedRunId.value = targetRunId;
    if (failed) auditLazyError.value = `${failed} audit detail panel(s) failed`;
  } catch (exc) {
    auditLazyError.value = exc.message;
  } finally {
    if (seq === auditLazySeq.value) auditSideLoading.value = false;
  }
}

async function ensureAuditLoaded() {
  if (activePage.value !== "audit") return;
  let targetRunId = runAudit.value?.run_id || "";
  if (!targetRunId) {
    targetRunId = await loadAuditCore();
  }
  if (auditDetailsRequested.value && targetRunId && auditSideLoadedRunId.value !== targetRunId && !auditSideLoading.value) {
    scheduleIdle(() => loadAuditSidePanels(targetRunId));
  }
}

async function requestAuditDetails() {
  auditDetailsRequested.value = true;
  let targetRunId = runAudit.value?.run_id || "";
  if (!targetRunId) {
    targetRunId = await loadAuditCore();
  }
  if (targetRunId && auditSideLoadedRunId.value !== targetRunId && !auditSideLoading.value) {
    await loadAuditSidePanels(targetRunId);
  }
}

function toggleAuditRawPayload() {
  showAuditRawPayload.value = !showAuditRawPayload.value;
}

async function refreshCore() {
  loading.value = true;
  error.value = "";
  try {
    const results = await Promise.allSettled([
      api.health(),
      api.config(),
      api.configProfiles(),
      api.pipelineStatusLite(),
      api.microStatus(),
      api.step15DaemonHealth(),
      api.runtimeWarmup(),
      api.pipelineWatchdog(),
      api.runtimeStatusLite(),
      api.pipelineFunnelLatest(true),
      api.configFieldImpactSummary(),
      api.configUiSchema(),
      ...strategyLineOrder.map((line) => api.configEffective(line))
    ]);
    const [h, c, cp, pl, m, sdh, rw, wd, r, pf, cis, cui, ...effectiveResults] = results;
    if (h.status === "fulfilled") health.value = h.value;
    if (c.status === "fulfilled") {
      config.value = c.value;
      syncConfigDrafts(c.value);
    }
    if (cp.status === "fulfilled") configProfiles.value = cp.value;
    if (pl.status === "fulfilled") pipeline.value = pl.value;
    if (pl.status === "fulfilled") setPipelineFromApi(pl.value);
    if (m.status === "fulfilled") micro.value = m.value;
    if (r.status === "fulfilled") runtime.value = r.value;
    if (sdh.status === "fulfilled") step15DaemonHealth.value = sdh.value;
    if (rw.status === "fulfilled") runtimeWarmup.value = rw.value;
    if (wd.status === "fulfilled") pipelineWatchdog.value = wd.value;
    if (pf.status === "fulfilled") pipelineFunnel.value = pf.value;
    if (cis.status === "fulfilled") configFieldImpactSummary.value = cis.value;
    if (cui.status === "fulfilled") configUiSchema.value = cui.value;
    const nextEffective = { ...configEffective.value };
    effectiveResults.forEach((item, index) => {
      if (item.status === "fulfilled") nextEffective[strategyLineOrder[index]] = item.value;
    });
    configEffective.value = nextEffective;
    log("REFRESH core snapshot");
  } catch (exc) {
    error.value = exc.message;
  } finally {
    loading.value = false;
  }
}

async function loadDashboardPage(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.paperSummaryLite(null, 10),
    api.paperDaemonStatus(),
    api.restHealth(),
    api.runtimeRestBudget(),
    api.strategy4Runtime(),
    api.strategy4ObservePool(),
    api.strategy5Runtime(20),
    api.strategy6Runtime(20),
    api.pipelineFunnelLatest(true),
    api.strategySandboxList({ limit: 50 }),
    api.strategySandboxActive()
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [p, ps, rh, rb, s4r, s4p, s5r, s6r, pf, sbl, sba] = results;
  if (p.status === "fulfilled") paper.value = p.value;
  if (ps.status === "fulfilled") paperStatus.value = ps.value;
  if (rh.status === "fulfilled") restHealth.value = rh.value;
  if (rb.status === "fulfilled") runtimeRestBudget.value = rb.value;
  if (s4r.status === "fulfilled") strategy4Runtime.value = s4r.value;
  if (s4p.status === "fulfilled") strategy4ObservePool.value = s4p.value;
  if (s5r.status === "fulfilled") strategy5Runtime.value = s5r.value;
  if (s6r.status === "fulfilled") strategy6Runtime.value = s6r.value;
  if (pf.status === "fulfilled") pipelineFunnel.value = pf.value;
  if (sbl.status === "fulfilled") sandboxLabList.value = sbl.value;
  if (sba.status === "fulfilled") {
    sandboxLabActive.value = sba.value;
    const activeId = sba.value?.active_sandbox_id || "";
    const sandboxRows = sandboxLabList.value?.sandboxes || [];
    const selectedExists = sandboxRows.some((item) => item.sandbox_id === sandboxLabSelectedId.value);
    if (activeId) {
      sandboxLabSelectedId.value = activeId;
    } else if (!selectedExists) {
      sandboxLabSelectedId.value = sandboxRows[0]?.sandbox_id || "";
    }
  }
}

async function loadTradePlansPage(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.tradePlans(),
    api.tradePlanFunnel("latest", 120),
    api.strategy4Runtime(),
    api.strategy4ObservePool(),
    api.strategy4Attempts(50),
    api.strategy5Runtime(50),
    api.strategy5Evidence(50),
    api.strategy6Runtime(50),
    api.strategy6Evidence(50)
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [t, tf, s4r, s4p, s4a, s5r, s5e, s6r, s6e] = results;
  if (t.status === "fulfilled") tradePlans.value = t.value;
  if (tf.status === "fulfilled") {
    tradePlanFunnel.value = tf.value;
    const lines = tf.value?.strategy_lines || [];
    if (!selectedTradePlanLine.value && lines.length) {
      selectedTradePlanLine.value = (lines.find((row) => !row.skipped) || lines[0]).line;
    }
  }
  if (s4r.status === "fulfilled") strategy4Runtime.value = s4r.value;
  if (s4p.status === "fulfilled") strategy4ObservePool.value = s4p.value;
  if (s4a.status === "fulfilled") strategy4Attempts.value = s4a.value;
  if (s5r.status === "fulfilled") strategy5Runtime.value = s5r.value;
  if (s5e.status === "fulfilled") strategy5Evidence.value = s5e.value;
  if (s6r.status === "fulfilled") strategy6Runtime.value = s6r.value;
  if (s6e.status === "fulfilled") strategy6Evidence.value = s6e.value;
}

async function loadPaperPage(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.paperSummaryLite(null, 20),
    api.paperDaemonStatus(),
    api.paperIntents(),
    api.paperEpochs(),
    api.paperExperiments(null, 40)
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [p, ps, pi, pec, pe] = results;
  if (p.status === "fulfilled") paper.value = p.value;
  if (ps.status === "fulfilled") paperStatus.value = ps.value;
  if (pi.status === "fulfilled") paperIntents.value = pi.value;
  if (pec.status === "fulfilled") paperEpochs.value = pec.value;
  if (pe.status === "fulfilled") paperExperiments.value = pe.value;
}

async function loadSnapshotPage(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.restHealth(),
    api.runtimeRestBudget(),
    api.step15Daemon(),
    api.step15DaemonHealth(),
    api.runtimeWarmup()
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [rh, rb, sd, sdh, rw] = results;
  if (rh.status === "fulfilled") restHealth.value = rh.value;
  if (rb.status === "fulfilled") runtimeRestBudget.value = rb.value;
  if (sd.status === "fulfilled") step15Daemon.value = sd.value;
  if (sdh.status === "fulfilled") step15DaemonHealth.value = sdh.value;
  if (rw.status === "fulfilled") runtimeWarmup.value = rw.value;
}

async function loadMicroPage(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.microStatus(),
    api.microTrainingLatest(160),
    api.microTrainingRuns(40),
    api.microTrainingCoverage()
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [m, latest, runs, coverage] = results;
  if (m.status === "fulfilled") micro.value = m.value;
  if (latest.status === "fulfilled") microTrainingLatest.value = latest.value;
  if (runs.status === "fulfilled") microTrainingRuns.value = runs.value;
  if (coverage.status === "fulfilled") microTrainingCoverage.value = coverage.value;
}

async function loadStrategy4Page(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.strategy4Runtime(),
    api.strategy4ObservePool(),
    api.strategy4Attempts(120),
    api.tradePlans()
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [s4r, s4p, s4a, t] = results;
  if (s4r.status === "fulfilled") strategy4Runtime.value = s4r.value;
  if (s4p.status === "fulfilled") strategy4ObservePool.value = s4p.value;
  if (s4a.status === "fulfilled") strategy4Attempts.value = s4a.value;
  if (t.status === "fulfilled") tradePlans.value = t.value;
}

async function loadStrategy5Page(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.strategy5Runtime(200),
    api.strategy5Evidence(200),
    api.tradePlans()
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [s5r, s5e, t] = results;
  if (s5r.status === "fulfilled") strategy5Runtime.value = s5r.value;
  if (s5e.status === "fulfilled") strategy5Evidence.value = s5e.value;
  if (t.status === "fulfilled") tradePlans.value = t.value;
}

async function loadStrategy6Page(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([
    api.strategy6Runtime(200),
    api.strategy6Evidence(200),
    api.strategy6Decisions(200),
    api.strategy6WaitPool(200),
    api.strategy6Attempts(200),
    api.strategy6Heartbeat(),
    api.strategy6Watchdog(),
    api.tradePlans()
  ]);
  if (seq !== pageLoadSeq.value) return;
  const [s6r, s6e, s6d, s6w, s6a, s6h, s6wd, t] = results;
  if (s6r.status === "fulfilled") strategy6Runtime.value = s6r.value;
  if (s6e.status === "fulfilled") strategy6Evidence.value = s6e.value;
  if (s6d.status === "fulfilled") strategy6Decisions.value = s6d.value;
  if (s6w.status === "fulfilled") strategy6WaitPool.value = s6w.value;
  if (s6a.status === "fulfilled") strategy6Attempts.value = s6a.value;
  if (s6h.status === "fulfilled") strategy6Heartbeat.value = s6h.value;
  if (s6wd.status === "fulfilled") strategy6Watchdog.value = s6wd.value;
  if (t.status === "fulfilled") tradePlans.value = t.value;
}

async function strategy6DaemonAction(action) {
  loading.value = true;
  try {
    if (action === "start") await api.strategy6DaemonStart();
    if (action === "stop") await api.strategy6DaemonStop();
    if (action === "recheck") await api.strategy6RecheckNow();
    if (action === "watchdog") strategy6Watchdog.value = await api.strategy6Watchdog();
    if (action === "recover") strategy6Watchdog.value = await api.strategy6WatchdogRecover();
    await loadStrategy6Page(pageLoadSeq.value);
  } catch (err) {
    error.value = err?.message || String(err);
  } finally {
    loading.value = false;
  }
}

async function loadNotificationsPage(seq = pageLoadSeq.value) {
  const results = await Promise.allSettled([api.feishuConfig(), api.feishuDeliveries()]);
  if (seq !== pageLoadSeq.value) return;
  const [f, d] = results;
  if (f.status === "fulfilled") feishu.value = f.value;
  if (d.status === "fulfilled") deliveries.value = d.value.deliveries || [];
}

function backtestLabParams() {
  const symbols = String(backtestLabFilters.value.symbols || "")
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
  return {
    strategy_line: backtestLabFilters.value.strategy_line || "all",
    days: Math.max(1, Math.min(90, Number(backtestLabFilters.value.days || 30))),
    max_symbols: Math.max(1, Math.min(600, Number(backtestLabFilters.value.max_symbols || 20))),
    max_sets: Math.max(1, Math.min(5000, Number(backtestLabFilters.value.max_sets || 120))),
    symbol_shard_size: Math.max(1, Math.min(200, Number(backtestLabFilters.value.symbol_shard_size || 25))),
    max_workers: Math.max(1, Math.min(32, Number(backtestLabFilters.value.max_workers || 1))),
    scheduler_mode: backtestLabFilters.value.scheduler_mode || "parameter_batch",
    symbols
  };
}

function backtestLabJobProgress() {
  return backtestLabJob.value?.progress || {};
}

function backtestLabProgressPct() {
  const progress = backtestLabJobProgress();
  const total = Number(progress.total_count || 0);
  const done = Number(progress.done_count || 0);
  if (!total) return 0;
  return Math.max(0, Math.min(100, Math.round((done / total) * 100)));
}

function isBacktestLabJobActive(job = backtestLabJob.value) {
  return ["running", "running_degraded", "stalled", "queued", "stopping"].includes(String(job?.status || ""));
}

function stopBacktestLabPolling() {
  if (backtestLabJobTimer.value) {
    clearInterval(backtestLabJobTimer.value);
    backtestLabJobTimer.value = null;
  }
}

function stopBacktestLabDiscovery() {
  if (backtestLabDiscoveryTimer.value) {
    clearInterval(backtestLabDiscoveryTimer.value);
    backtestLabDiscoveryTimer.value = null;
  }
}

async function refreshBacktestLabJob(jobId = backtestLabJob.value?.job_id) {
  if (!jobId) return;
  const job = await api.backtestP21V2JobStatus(jobId);
  backtestLabJob.value = job;
  if (!isBacktestLabJobActive(job)) {
    stopBacktestLabPolling();
    const [experiments, leaderboard] = await Promise.allSettled([
      api.backtestP21V2Experiments(20),
      api.backtestP21V2Leaderboard(50)
    ]);
    if (experiments.status === "fulfilled") backtestLabExperiments.value = experiments.value;
    if (leaderboard.status === "fulfilled") backtestLabLeaderboard.value = leaderboard.value;
  }
}

function startBacktestLabPolling(jobId) {
  stopBacktestLabDiscovery();
  stopBacktestLabPolling();
  refreshBacktestLabJob(jobId).catch((exc) => {
    backtestLabMessage.value = exc.message;
  });
  backtestLabJobTimer.value = setInterval(() => {
    refreshBacktestLabJob(jobId).catch((exc) => {
      backtestLabMessage.value = exc.message;
    });
  }, 3000);
}

async function discoverBacktestLabJob() {
  if (activePage.value !== "backtest-lab") return;
  if (backtestLabJob.value && isBacktestLabJobActive(backtestLabJob.value)) return;
  const jobs = await api.backtestP21V2Jobs(10);
  backtestLabJobs.value = jobs;
  const running = (jobs.jobs || []).find((item) => isBacktestLabJobActive(item));
  if (running) {
    backtestLabJob.value = running;
    startBacktestLabPolling(running.job_id);
  }
}

function startBacktestLabDiscovery() {
  if (backtestLabDiscoveryTimer.value) return;
  discoverBacktestLabJob().catch((exc) => {
    backtestLabMessage.value = exc.message;
  });
  backtestLabDiscoveryTimer.value = setInterval(() => {
    discoverBacktestLabJob().catch((exc) => {
      backtestLabMessage.value = exc.message;
    });
  }, 5000);
}

async function loadBacktestLabPage(seq = pageLoadSeq.value) {
  backtestLabLoading.value = true;
  backtestLabAction.value = "loading";
  try {
    const params = backtestLabParams();
    const [status, contracts, experiments, leaderboard, jobs, tqJobs, s4Summary, s4Pool, s4Attempts] = await Promise.allSettled([
      api.backtestP21V2KlineStatus({ days: params.days, max_symbols: params.max_symbols, symbols: params.symbols.join(",") }),
      api.backtestP21V2MatrixContracts({ strategy_line: params.strategy_line, max_sets: params.max_sets }),
      api.backtestP21V2Experiments(20),
      api.backtestP21V2Leaderboard(50),
      api.backtestP21V2Jobs(10),
      api.backtestP21V2OpsTqJobs({ limit: 20 }),
      api.backtestP21V2Strategy4ReplaySummary(),
      api.backtestP21V2Strategy4ReplayPool({ limit: 20 }),
      api.backtestP21V2Strategy4ReplayAttempts({ limit: 20 })
    ]);
    if (seq !== pageLoadSeq.value) return;
    if (status.status === "fulfilled") backtestLabKlineStatus.value = status.value;
    if (contracts.status === "fulfilled") backtestLabMatrixContract.value = contracts.value;
    if (experiments.status === "fulfilled") backtestLabExperiments.value = experiments.value;
    if (leaderboard.status === "fulfilled") backtestLabLeaderboard.value = leaderboard.value;
    if (tqJobs.status === "fulfilled") backtestLabTqJobs.value = tqJobs.value;
    if (s4Summary.status === "fulfilled") backtestLabStrategy4Replay.value = s4Summary.value;
    if (s4Pool.status === "fulfilled") backtestLabStrategy4ReplayPool.value = s4Pool.value;
    if (s4Attempts.status === "fulfilled") backtestLabStrategy4ReplayAttempts.value = s4Attempts.value;
    if (jobs.status === "fulfilled") {
      backtestLabJobs.value = jobs.value;
      const running = (jobs.value.jobs || []).find((item) => isBacktestLabJobActive(item));
      if (running) {
        backtestLabJob.value = running;
        startBacktestLabPolling(running.job_id);
      } else {
        startBacktestLabDiscovery();
      }
    }
    backtestLabMessage.value = "";
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    if (seq === pageLoadSeq.value) {
      backtestLabLoading.value = false;
      backtestLabAction.value = "";
    }
  }
}

async function runBacktestLabStrategy4Replay() {
  if (backtestLabLoading.value) return;
  backtestLabLoading.value = true;
  backtestLabAction.value = "strategy4 replay";
  try {
    const params = backtestLabParams();
    const result = await api.backtestP21V2Strategy4ReplayRun({
      symbols: params.symbols.length ? params.symbols : null,
      days: Math.min(Number(params.days || 3), 7),
      max_symbols: Math.min(Number(params.max_symbols || 5), 10),
      max_sets: 1,
      max_admissions_per_symbol: 20,
      max_attempts: 12,
      observe_interval_min: 5,
      write: true
    });
    backtestLabStrategy4ReplayRun.value = result;
    backtestLabMessage.value = `strategy4 replay completed: ${result.experiment_id}`;
    const [summary, pool, attempts, leaderboard] = await Promise.all([
      api.backtestP21V2Strategy4ReplaySummary({ experiment_id: result.experiment_id }),
      api.backtestP21V2Strategy4ReplayPool({ experiment_id: result.experiment_id, limit: 30 }),
      api.backtestP21V2Strategy4ReplayAttempts({ experiment_id: result.experiment_id, limit: 30 }),
      api.backtestP21V2Leaderboard(50)
    ]);
    backtestLabStrategy4Replay.value = summary;
    backtestLabStrategy4ReplayPool.value = pool;
    backtestLabStrategy4ReplayAttempts.value = attempts;
    backtestLabLeaderboard.value = leaderboard;
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabLoading.value = false;
    backtestLabAction.value = "";
  }
}

async function startBacktestLabJob(jobType) {
  if (backtestLabLoading.value) return;
  backtestLabLoading.value = true;
  backtestLabAction.value = jobType;
  try {
    const params = backtestLabParams();
    const job = await api.backtestP21V2StartJob({
      job_type: jobType,
      symbols: params.symbols.length ? params.symbols : null,
      strategy_line: params.strategy_line,
      days: params.days,
      max_symbols: params.max_symbols,
      max_sets: params.max_sets,
      symbol_shard_size: params.symbol_shard_size,
      max_workers: params.max_workers,
      scheduler_mode: params.scheduler_mode,
      sleep_sec: jobType === "kline_download" ? 0.6 : 0.2
    });
    backtestLabJob.value = job;
    backtestLabMessage.value = `${jobType} job started`;
    startBacktestLabPolling(job.job_id);
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabLoading.value = false;
    backtestLabAction.value = "";
  }
}

async function stopBacktestLabJob() {
  const jobId = backtestLabJob.value?.job_id;
  if (!jobId) return;
  backtestLabLoading.value = true;
  backtestLabAction.value = "stopping";
  try {
    backtestLabJob.value = await api.backtestP21V2StopJob(jobId);
    backtestLabMessage.value = "stop requested";
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabLoading.value = false;
    backtestLabAction.value = "";
  }
}

async function runBacktestLabBaseline() {
  if (backtestLabLoading.value) return;
  backtestLabLoading.value = true;
  backtestLabAction.value = "download";
  try {
    const params = backtestLabParams();
    backtestLabBaseline.value = await api.backtestP21V2DownloadKlines({
      symbols: params.symbols.length ? params.symbols : null,
      days: params.days,
      max_symbols: params.max_symbols,
      dry_run: false,
      sleep_sec: 0.05
    });
    backtestLabKlineStatus.value = await api.backtestP21V2KlineStatus({ days: params.days, max_symbols: params.max_symbols, symbols: params.symbols.join(",") });
    backtestLabMessage.value = "kline cache download completed";
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabLoading.value = false;
    backtestLabAction.value = "";
  }
}

async function runBacktestLabMatrix() {
  if (backtestLabLoading.value) return;
  backtestLabLoading.value = true;
  backtestLabAction.value = "matrix";
  try {
    const params = backtestLabParams();
    backtestLabMatrix.value = await api.backtestP21V2RunMatrix({
      symbols: params.symbols.length ? params.symbols : null,
      strategy_line: params.strategy_line,
      days: params.days,
      max_symbols: params.max_symbols,
      max_sets: params.max_sets,
      write: true
    });
    backtestLabMessage.value = "matrix completed";
    const [experiments, leaderboard] = await Promise.all([
      api.backtestP21V2Experiments(20),
      api.backtestP21V2Leaderboard(20)
    ]);
    backtestLabExperiments.value = experiments;
    backtestLabLeaderboard.value = leaderboard;
    backtestLabRecommendations.value = { count: backtestLabMatrix.value.recommendations?.length || 0, recommendations: backtestLabMatrix.value.recommendations || [] };
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabLoading.value = false;
    backtestLabAction.value = "";
  }
}

async function openBacktestLabExperiment(experimentId) {
  if (!experimentId) return;
  backtestLabSelectedExperiment.value = experimentId;
  try {
    const [detail, daily, symbols, orders] = await Promise.allSettled([
      api.backtestP21V2Experiment(experimentId),
      api.backtestP21V2ExperimentDaily(experimentId, { limit: 120 }),
      api.backtestP21V2ExperimentSymbols(experimentId, { limit: 120 }),
      api.backtestP21V2ExperimentOrders(experimentId, { limit: 80 })
    ]);
    if (detail.status === "fulfilled") backtestLabExperimentDetail.value = detail.value;
    if (daily.status === "fulfilled") backtestLabExperimentDaily.value = daily.value;
    if (symbols.status === "fulfilled") backtestLabExperimentSymbols.value = symbols.value;
    if (orders.status === "fulfilled") backtestLabExperimentOrders.value = orders.value;
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  }
}

function backtestLabQualityParams(extra = {}) {
  const selection = backtestLabQualitySelection.value || {};
  return {
    experiment_id: selection.experiment_id,
    strategy_line: selection.strategy_line,
    parameter_set_id: selection.parameter_set_id,
    ...extra
  };
}

async function loadBacktestLabQuality() {
  const selection = backtestLabQualitySelection.value;
  if (!selection?.experiment_id) return;
  backtestLabQualityLoading.value = true;
  try {
    const params = backtestLabQualityParams({ limit: 5000 });
    const [summary, aggregates, samples] = await Promise.allSettled([
      api.backtestP21V2QualitySummary(params),
      api.backtestP21V2QualityAggregates(backtestLabQualityParams({ limit: 200 })),
      api.backtestP21V2QualitySamples(backtestLabQualityParams({ limit: 80 }))
    ]);
    if (summary.status === "fulfilled") backtestLabQualitySummary.value = summary.value;
    if (aggregates.status === "fulfilled") backtestLabQualityAggregates.value = aggregates.value;
    if (samples.status === "fulfilled") backtestLabQualitySamples.value = samples.value;
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabQualityLoading.value = false;
  }
}

async function openBacktestLabQuality(row) {
  if (!row?.experiment_id) return;
  backtestLabQualitySelection.value = {
    experiment_id: row.experiment_id,
    strategy_line: row.strategy_line || row.parameters?.strategy_line || "",
    parameter_set_id: row.parameter_set_id || ""
  };
  backtestLabQualityDryRun.value = null;
  await loadBacktestLabQuality();
}

async function materializeBacktestLabQuality(dryRun = false) {
  const selection = backtestLabQualitySelection.value;
  if (!selection?.experiment_id || backtestLabQualityLoading.value) return;
  backtestLabQualityLoading.value = true;
  try {
    const request = {
      source_type: "backtest",
      experiment_id: selection.experiment_id,
      strategy_line: selection.strategy_line || null,
      parameter_set_id: selection.parameter_set_id || null,
      top_n: selection.parameter_set_id ? 1 : 3,
      limit: 5000,
      dry_run: dryRun,
      force: false,
      include_v5: true,
      include_gates: true,
      min_samples: 50,
      gate_limit: 120
    };
    const result = dryRun
      ? await api.backtestP21V2QualityMaterialize(request)
      : await api.backtestP21V2OpsTqJobEnqueue(request);
    backtestLabQualityDryRun.value = result;
    backtestLabMessage.value = dryRun ? `quality dry-run: ${result.selected_order_count || 0} orders` : `TQ async job ${result.status}: ${result.job_id || "-"}`;
    if (!dryRun) await loadBacktestLabTqJobs();
  } catch (exc) {
    backtestLabMessage.value = exc.message;
  } finally {
    backtestLabQualityLoading.value = false;
  }
}

async function loadBacktestLabTqJobs() {
  if (backtestLabTqJobLoading.value) return;
  backtestLabTqJobLoading.value = true;
  try {
    backtestLabTqJobs.value = await api.backtestP21V2OpsTqJobs({ limit: 30 });
    backtestLabTqJobMessage.value = "TQ jobs refreshed";
  } catch (exc) {
    backtestLabTqJobMessage.value = exc.message;
  } finally {
    backtestLabTqJobLoading.value = false;
  }
}

async function processNextBacktestLabTqJob() {
  if (backtestLabTqJobLoading.value) return;
  backtestLabTqJobLoading.value = true;
  try {
    const result = await api.backtestP21V2OpsTqJobProcessNext();
    backtestLabTqJobMessage.value = result.status === "idle" ? "no queued TQ job" : `processed ${result.job_id}: ${result.status}`;
    backtestLabTqJobs.value = await api.backtestP21V2OpsTqJobs({ limit: 30 });
    if (result.status === "done") {
      await Promise.allSettled([loadBacktestLabQuality(), loadBacktestLabGateScoring()]);
    }
  } catch (exc) {
    backtestLabTqJobMessage.value = exc.message;
  } finally {
    backtestLabTqJobLoading.value = false;
  }
}

function backtestLabGateExperimentId() {
  return backtestLabGateFilters.value.experiment_id
    || backtestLabQualitySelection.value?.experiment_id
    || backtestLabSelectedExperiment.value
    || (backtestLabLeaderboard.value?.leaderboard || [])[0]?.experiment_id
    || "";
}

function backtestLabGateParameterSetId() {
  return backtestLabGateFilters.value.parameter_set_id
    || backtestLabQualitySelection.value?.parameter_set_id
    || "";
}

function backtestLabGateParams(extra = {}) {
  return {
    experiment_id: backtestLabGateExperimentId() || undefined,
    strategy_line: backtestLabGateFilters.value.strategy_line || "all",
    parameter_set_id: backtestLabGateParameterSetId() || undefined,
    limit: Math.max(1, Math.min(100000, Number(backtestLabGateFilters.value.limit || 500))),
    ...extra
  };
}

async function loadBacktestLabGateScoring() {
  if (backtestLabGateLoading.value) return;
  backtestLabGateLoading.value = true;
  try {
    const params = backtestLabGateParams({ limit: 300 });
    const [features, buckets, scores, candidates, recommendations] = await Promise.allSettled([
      api.backtestP21V2GateFeatures(params),
      api.backtestP21V2GateBuckets(params),
      api.backtestP21V2GateScores(params),
      api.backtestP21V2GateCandidates(params),
      api.backtestP21V2GateRecommendations(params)
    ]);
    if (features.status === "fulfilled") backtestLabGateFeatures.value = features.value;
    if (buckets.status === "fulfilled") backtestLabGateBuckets.value = buckets.value;
    if (scores.status === "fulfilled") backtestLabGateScores.value = scores.value;
    if (candidates.status === "fulfilled") backtestLabGateCandidates.value = candidates.value;
    if (recommendations.status === "fulfilled") backtestLabGateRecommendations.value = recommendations.value;
    backtestLabGateMessage.value = "gate/scoring refreshed";
  } catch (exc) {
    backtestLabGateMessage.value = exc.message;
  } finally {
    backtestLabGateLoading.value = false;
  }
}

async function runBacktestLabGatePipeline(dryRun = false) {
  if (backtestLabGateLoading.value) return;
  backtestLabGateLoading.value = true;
  try {
    const base = backtestLabGateParams();
    backtestLabGateBatch.value = await api.backtestP21V2GateTqBatchMaterialize({
      experiment_id: base.experiment_id,
      strategy_line: base.strategy_line,
      top_n: Math.max(1, Math.min(30, Number(backtestLabGateFilters.value.top_n || 5))),
      limit: Math.max(1, Math.min(5000, Number(backtestLabGateFilters.value.limit || 500))),
      dry_run: dryRun
    });
    backtestLabGateFeatureBuild.value = await api.backtestP21V2GateFeaturesMaterialize({
      experiment_id: base.experiment_id,
      strategy_line: base.strategy_line,
      parameter_set_id: base.parameter_set_id,
      limit: Math.max(1, Math.min(100000, Number(backtestLabGateFilters.value.limit || 500))),
      dry_run: dryRun
    });
    backtestLabGateBucketBuild.value = await api.backtestP21V2GateBucketsRebuild({
      experiment_id: base.experiment_id,
      strategy_line: base.strategy_line,
      parameter_set_id: base.parameter_set_id,
      limit: Math.max(1, Math.min(100000, Number(backtestLabGateFilters.value.limit || 500))),
      min_samples: Math.max(1, Math.min(500, Number(backtestLabGateFilters.value.min_samples || 5))),
      dry_run: dryRun
    });
    backtestLabGateScoreBuild.value = await api.backtestP21V2GateScoresRebuild({
      experiment_id: base.experiment_id,
      strategy_line: base.strategy_line,
      parameter_set_id: base.parameter_set_id,
      limit: Math.max(1, Math.min(100000, Number(backtestLabGateFilters.value.limit || 500))),
      min_samples: Math.max(1, Math.min(500, Number(backtestLabGateFilters.value.min_samples || 5))),
      dry_run: dryRun
    });
    backtestLabGateCandidateBuild.value = await api.backtestP21V2GateCandidatesGenerate({
      experiment_id: base.experiment_id,
      strategy_line: base.strategy_line,
      parameter_set_id: base.parameter_set_id,
      limit: Math.max(1, Math.min(100000, Number(backtestLabGateFilters.value.limit || 500))),
      min_samples: Math.max(1, Math.min(500, Number(backtestLabGateFilters.value.min_samples || 5))),
      min_test_pf: Math.max(0, Math.min(1000, Number(backtestLabGateFilters.value.min_test_pf || 1))),
      min_coverage: Math.max(0, Math.min(1, Number(backtestLabGateFilters.value.min_coverage || 0.05))),
      dry_run: dryRun
    });
    backtestLabGateMessage.value = dryRun ? "shadow gate dry-run completed" : "shadow gate candidates generated";
    backtestLabGateLoading.value = false;
    await loadBacktestLabGateScoring();
  } catch (exc) {
    backtestLabGateMessage.value = exc.message;
    backtestLabGateLoading.value = false;
  }
}

function parseSandboxJson(raw, fallback = {}) {
  try {
    return JSON.parse(raw || "{}");
  } catch {
    return fallback;
  }
}

function sandboxTagList(raw) {
  return String(raw || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function sandboxBranchLines() {
  return (sandboxLabBranches.value?.branches || []).map((branch) => branch.strategy_line).filter(Boolean);
}

async function loadSandboxCodeOverlay(sandboxId = sandboxLabSelectedId.value, strategyLine = sandboxLabSelectedBranch.value) {
  if (!sandboxId || !strategyLine) {
    sandboxLabCodeOverlay.value = null;
    return;
  }
  sandboxLabCodeOverlay.value = await api.strategySandboxCodeOverlay(sandboxId, strategyLine);
}

async function loadSandboxLabPage(seq = pageLoadSeq.value) {
  sandboxLabLoading.value = true;
  try {
    const [list, active] = await Promise.allSettled([
      api.strategySandboxList(sandboxLabFilters.value),
      api.strategySandboxActive()
    ]);
    if (seq !== pageLoadSeq.value) return;
    if (list.status === "fulfilled") sandboxLabList.value = list.value;
    if (active.status === "fulfilled") {
      sandboxLabActive.value = active.value;
      const activeId = active.value?.active_sandbox_id || "";
      const sandboxRows = sandboxLabRows.value;
      const selectedExists = sandboxRows.some((item) => item.sandbox_id === sandboxLabSelectedId.value);
      if (activeId && sandboxRows.some((item) => item.sandbox_id === activeId)) sandboxLabSelectedId.value = activeId;
      else if (!selectedExists) sandboxLabSelectedId.value = sandboxRows[0]?.sandbox_id || "";
    }
    const selected = sandboxLabSelectedId.value || (sandboxLabActive.value?.active_sandbox_id && sandboxLabRows.value.some((item) => item.sandbox_id === sandboxLabActive.value.active_sandbox_id) ? sandboxLabActive.value.active_sandbox_id : "") || sandboxLabRows.value[0]?.sandbox_id;
    if (selected) {
      sandboxLabSelectedId.value = selected;
      const [summary, health, branches, leaderboard, tqCompare, gateCompare] = await Promise.allSettled([
        api.strategySandboxSummary(selected),
        api.strategySandboxDbHealth(selected),
        api.strategySandboxBranches(selected),
        api.strategySandboxLeaderboard(selected),
        api.strategySandboxTradeQualityCompare(selected),
        api.strategySandboxGateCompare(selected)
      ]);
      if (seq !== pageLoadSeq.value) return;
      if (summary.status === "fulfilled") sandboxLabSummary.value = summary.value.summary;
      if (health.status === "fulfilled") sandboxLabHealth.value = health.value.health;
      if (branches.status === "fulfilled") sandboxLabBranches.value = branches.value;
      if (leaderboard.status === "fulfilled") sandboxLabLeaderboard.value = leaderboard.value;
      if (tqCompare.status === "fulfilled") sandboxLabTqCompare.value = tqCompare.value;
      if (gateCompare.status === "fulfilled") sandboxLabGateCompare.value = gateCompare.value;
      const lines = sandboxBranchLines();
      if (!lines.includes(sandboxLabSelectedBranch.value)) sandboxLabSelectedBranch.value = lines[0] || "strategy5";
      if (sandboxLabSelectedBranch.value) {
        try {
          await loadSandboxCodeOverlay(selected, sandboxLabSelectedBranch.value);
        } catch {
          sandboxLabCodeOverlay.value = null;
        }
      }
    }
    sandboxLabMessage.value = "";
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    if (seq === pageLoadSeq.value) sandboxLabLoading.value = false;
  }
}

function researchDbParams() {
  return {
    strategy_line: researchDbFilters.value.strategy_line || "all",
    source_type: researchDbFilters.value.source_type || "all",
    limit: Math.max(20, Math.min(500, Number(researchDbFilters.value.limit || 100))),
    offset: 0
  };
}

async function loadResearchDbPage(seq = pageLoadSeq.value) {
  researchDbLoading.value = true;
  try {
    const params = researchDbParams();
    const [summary, tradeFacts, entryFeatures, tqSamples, datasetCards, writerStatus, fieldCoverage, lineageAudit] = await Promise.allSettled([
      api.researchDbSummary(),
      api.researchDbTradeFacts(params),
      api.researchDbEntryFeatures(params),
      api.researchDbTqSamples(params),
      api.researchDbDatasetCards(10),
      api.researchDbWriterStatus(),
      api.researchDbFieldCoverage(params),
      api.researchDbLineageAudit()
    ]);
    if (seq !== pageLoadSeq.value) return;
    if (summary.status === "fulfilled") researchDb.value.summary = summary.value;
    if (tradeFacts.status === "fulfilled") researchDb.value.tradeFacts = tradeFacts.value;
    if (entryFeatures.status === "fulfilled") researchDb.value.entryFeatures = entryFeatures.value;
    if (tqSamples.status === "fulfilled") researchDb.value.tqSamples = tqSamples.value;
    if (datasetCards.status === "fulfilled") researchDb.value.datasetCards = datasetCards.value;
    if (writerStatus.status === "fulfilled") researchDb.value.writerStatus = writerStatus.value;
    if (fieldCoverage.status === "fulfilled") researchDb.value.fieldCoverage = fieldCoverage.value;
    if (lineageAudit.status === "fulfilled") researchDb.value.lineageAudit = lineageAudit.value;
    researchDbMessage.value = "";
  } catch (exc) {
    researchDbMessage.value = exc.message;
  } finally {
    if (seq === pageLoadSeq.value) researchDbLoading.value = false;
  }
}

async function materializeResearchDb(dryRun = false) {
  if (researchDbLoading.value) return;
  researchDbLoading.value = true;
  try {
    const result = await api.researchDbMaterialize({ dry_run: dryRun, limit: dryRun ? 1000 : "" });
    researchDbMessage.value = `${dryRun ? "Dry run" : "Materialize"} completed: ${JSON.stringify(result.rows || {})}`;
    await loadResearchDbPage(pageLoadSeq.value);
  } catch (exc) {
    researchDbMessage.value = exc.message;
  } finally {
    researchDbLoading.value = false;
  }
}

async function createSandboxPlan() {
  if (sandboxLabLoading.value) return;
  sandboxLabLoading.value = true;
  try {
    const created = await api.strategySandboxCreate({
      strategy_line: sandboxLabCreateDraft.value.strategy_line,
      strategy_lines: sandboxLabCreateDraft.value.strategy_lines,
      strategy_version: sandboxLabCreateDraft.value.strategy_version || "review",
      data_scope: parseSandboxJson(sandboxLabCreateDraft.value.data_scope, { mode: "invalid_json" }),
      config_scope: parseSandboxJson(sandboxLabCreateDraft.value.config_scope, { mode: "invalid_json" }),
      tags: sandboxTagList(sandboxLabCreateDraft.value.tags)
    });
    const sandboxId = created?.sandbox?.sandbox_id;
    if (sandboxId) {
      sandboxLabSelectedId.value = sandboxId;
      if (sandboxLabCreateDraft.value.set_active_after_create) {
        sandboxLabActive.value = await api.strategySandboxSetActive(sandboxId);
      }
    }
    sandboxLabMessage.value = `sandbox created: ${sandboxId || "-"}${sandboxLabCreateDraft.value.set_active_after_create ? " and set active" : " (not active)"}`;
    await loadSandboxLabPage(pageLoadSeq.value);
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function selectSandboxPlan(sandboxId) {
  if (!sandboxId || sandboxLabLoading.value) return;
  const target = sandboxLabRows.value.find((item) => item.sandbox_id === sandboxId);
  if (!target || target.status === "deleted") {
    sandboxLabMessage.value = "deleted or missing sandbox cannot be selected";
    return;
  }
  sandboxLabLoading.value = true;
  try {
    sandboxLabSelectedId.value = sandboxId;
    sandboxLabActive.value = await api.strategySandboxSetActive(sandboxId);
    const [summary, health, branches, leaderboard, tqCompare, gateCompare] = await Promise.all([
      api.strategySandboxSummary(sandboxId),
      api.strategySandboxDbHealth(sandboxId),
      api.strategySandboxBranches(sandboxId),
      api.strategySandboxLeaderboard(sandboxId),
      api.strategySandboxTradeQualityCompare(sandboxId),
      api.strategySandboxGateCompare(sandboxId)
    ]);
    sandboxLabSummary.value = summary.summary;
    sandboxLabHealth.value = health.health;
    sandboxLabBranches.value = branches;
    sandboxLabLeaderboard.value = leaderboard;
    sandboxLabTqCompare.value = tqCompare;
    sandboxLabGateCompare.value = gateCompare;
    const lines = sandboxBranchLines();
    if (!lines.includes(sandboxLabSelectedBranch.value)) sandboxLabSelectedBranch.value = lines[0] || "strategy5";
    await loadSandboxCodeOverlay(sandboxId, sandboxLabSelectedBranch.value);
    sandboxLabMessage.value = `active sandbox: ${sandboxId}`;
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function runSandboxJob(jobType) {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId) {
    sandboxLabMessage.value = "select a sandbox before running jobs";
    return;
  }
  if (sandboxLabLoading.value || sandboxLabJobRunning.value) return;
  sandboxLabLoading.value = true;
  try {
    sandboxLabLastJob.value = await api.strategySandboxMultiBranchJob(sandboxId, jobType, { source: "vue3_sandbox_lab", strategy_line: "all" });
    const [summary, leaderboard, tqCompare, gateCompare] = await Promise.all([
      api.strategySandboxSummary(sandboxId),
      api.strategySandboxLeaderboard(sandboxId),
      api.strategySandboxTradeQualityCompare(sandboxId),
      api.strategySandboxGateCompare(sandboxId)
    ]);
    sandboxLabSummary.value = summary.summary;
    sandboxLabLeaderboard.value = leaderboard;
    sandboxLabTqCompare.value = tqCompare;
    sandboxLabGateCompare.value = gateCompare;
    sandboxLabMessage.value = `${jobType} completed`;
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function deleteSandboxPlan(mode = "soft_delete") {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId) {
    sandboxLabMessage.value = "select a sandbox before delete";
    return;
  }
  if (mode !== "soft_delete") {
    sandboxLabMessage.value = "purge is not exposed in the standard UI";
    return;
  }
  if (sandboxLabLoading.value || sandboxLabJobRunning.value) return;
  const confirmed = window.confirm(`Soft delete sandbox ${sandboxId}? This clears the active analysis context when it points to this sandbox.`);
  if (!confirmed) return;
  sandboxLabLoading.value = true;
  try {
    sandboxLabLastJob.value = await api.strategySandboxDelete(sandboxId, {
      mode,
      reason: "vue3_sandbox_lab",
      confirm: mode === "purge"
    });
    sandboxLabSelectedId.value = "";
    sandboxLabSummary.value = null;
    sandboxLabHealth.value = null;
    sandboxLabBranches.value = { branches: [], count: 0 };
    sandboxLabLeaderboard.value = { leaderboard: [], count: 0 };
    sandboxLabTqCompare.value = { items: [], count: 0 };
    sandboxLabGateCompare.value = { items: [], count: 0 };
    sandboxLabCodeOverlay.value = null;
    sandboxLabMessage.value = `${mode} sandbox: ${sandboxId}`;
    await loadSandboxLabPage(pageLoadSeq.value);
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function changeSandboxBranchForOverlay() {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId || sandboxLabLoading.value) return;
  sandboxLabLoading.value = true;
  try {
    await loadSandboxCodeOverlay(sandboxId, sandboxLabSelectedBranch.value);
    sandboxLabMessage.value = `branch overlay loaded: ${sandboxLabSelectedBranch.value}`;
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function createSandboxCodeOverlay() {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId || !sandboxLabSelectedBranch.value || sandboxLabActionDisabled.value) return;
  sandboxLabLoading.value = true;
  try {
    sandboxLabCodeOverlay.value = await api.strategySandboxCreateCodeOverlay(sandboxId, sandboxLabSelectedBranch.value);
    sandboxLabMessage.value = `code overlay ready: ${sandboxLabSelectedBranch.value}`;
    await loadSandboxLabPage(pageLoadSeq.value);
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function addSandboxCodePatch() {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId || !sandboxLabSelectedBranch.value || sandboxLabActionDisabled.value) return;
  sandboxLabLoading.value = true;
  try {
    sandboxLabCodeOverlay.value = await api.strategySandboxAddCodePatch(sandboxId, sandboxLabSelectedBranch.value, {
      patch_type: "manifest_note",
      target_relpath: sandboxLabPatchDraft.value.target_relpath || "notes/strategy-experiment.md",
      note: sandboxLabPatchDraft.value.note,
      diff_text: sandboxLabPatchDraft.value.diff_text,
      patch_json: { source: "vue3_sandbox_lab", note: sandboxLabPatchDraft.value.note }
    });
    sandboxLabMessage.value = `code patch added: ${sandboxLabSelectedBranch.value}`;
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function buildSandboxRuntime() {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId || !sandboxLabSelectedBranch.value || sandboxLabActionDisabled.value) return;
  sandboxLabLoading.value = true;
  try {
    sandboxLabCodeOverlay.value = await api.strategySandboxBuildRuntime(sandboxId, sandboxLabSelectedBranch.value, {});
    sandboxLabMessage.value = `runtime built: ${sandboxLabCodeOverlay.value?.runtime_id || "-"}`;
    await loadSandboxLabPage(pageLoadSeq.value);
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}

async function smokeSandboxRuntime() {
  const sandboxId = sandboxLabSelectedId.value || sandboxLabActive.value?.active_sandbox_id;
  if (!sandboxId || !sandboxLabSelectedBranch.value || sandboxLabActionDisabled.value) return;
  sandboxLabLoading.value = true;
  try {
    sandboxLabLastJob.value = await api.strategySandboxRuntimeSmoke(sandboxId, sandboxLabSelectedBranch.value, { symbols: ["BTCUSDT"] });
    sandboxLabMessage.value = `runtime smoke passed: ${sandboxLabSelectedBranch.value}`;
    await loadSandboxLabPage(pageLoadSeq.value);
  } catch (exc) {
    sandboxLabMessage.value = exc.message;
  } finally {
    sandboxLabLoading.value = false;
  }
}


async function loadAuditPage(seq = pageLoadSeq.value) {
  try {
    await loadAuditCore();
    if (seq !== pageLoadSeq.value) return;
  } catch (exc) {
    auditLazyError.value = exc.message;
  }
}

async function loadActivePageData(seq = pageLoadSeq.value) {
  const page = activePage.value;
  if (page === "dashboard") return loadDashboardPage(seq);
  if (page === "plans") return loadTradePlansPage(seq);
  if (page === "paper") return loadPaperPage(seq);
  if (page === "audit") return loadAuditPage(seq);
  if (page === "snapshot") return loadSnapshotPage(seq);
  if (page === "micro") return loadMicroPage(seq);
  if (page === "strategy4") return loadStrategy4Page(seq);
  if (page === "strategy5") return loadStrategy5Page(seq);
  if (page === "strategy6") return loadStrategy6Page(seq);
  if (page === "notifications") return loadNotificationsPage(seq);
  if (page === "trade-quality") return refreshTradeQuality();
  if (page === "backtest-lab") return loadBacktestLabPage(seq);
  if (page === "sandbox-lab") return loadSandboxLabPage(seq);
  if (page === "research-db") return loadResearchDbPage(seq);
  if (page === "pipeline") return Promise.allSettled([
    api.pipelineStatusLite(),
    api.pipelineFunnelLatest(true)
  ]).then((results) => {
    if (seq !== pageLoadSeq.value) return;
    const [status, funnel] = results;
    if (status.status === "fulfilled") {
      pipeline.value = status.value;
      setPipelineFromApi(status.value);
    }
    if (funnel.status === "fulfilled") pipelineFunnel.value = funnel.value;
  });
  return Promise.resolve();
}

async function refreshAll() {
  const seq = pageLoadSeq.value + 1;
  pageLoadSeq.value = seq;
  await refreshCore();
  await loadActivePageData(seq);
}

async function refreshPaperRealtime() {
  try {
    const line = paperTab.value;
    const [summary, status, consumption, realism, reconciliation] = await Promise.allSettled([
      api.paperSummaryLite(null, 50),
      api.paperDaemonStatus(),
      api.paperConsumptionStatus(undefined, 50),
      api.paperRealismMetrics(line),
      api.paperReconciliation(line, 120)
    ]);
    if (summary.status === "fulfilled") paper.value = summary.value;
    if (status.status === "fulfilled") paperStatus.value = status.value;
    if (consumption.status === "fulfilled") paperConsumption.value = consumption.value;
    if (realism.status === "fulfilled") paperRealism.value = realism.value;
    if (reconciliation.status === "fulfilled") paperReconciliation.value = reconciliation.value;
    const now = new Date();
    paperLastRefreshAt.value = now.toLocaleTimeString();
    paperNextRefreshAt.value = new Date(now.getTime() + 60_000).toLocaleTimeString();
  } catch (exc) {
    error.value = exc.message;
  }
}

function tradeQualityParams() {
  const limit = Math.min(500, Math.max(20, Number(tradeQualityFilters.value.limit || 200)));
  if (tradeQualityFilters.value.limit !== limit) tradeQualityFilters.value.limit = limit;
  return {
    ...tradeQualityFilters.value,
    symbol: String(tradeQualityFilters.value.symbol || "").trim().toUpperCase(),
    archive_id: String(tradeQualityFilters.value.archive_id || "").trim(),
    package_key: String(tradeQualityFilters.value.package_key || "").trim(),
    experiment_id: String(tradeQualityFilters.value.experiment_id || "").trim(),
    parameter_set_id: String(tradeQualityFilters.value.parameter_set_id || "").trim(),
    backtest_package_strategy_line: String(tradeQualityFilters.value.backtest_package_strategy_line || "").trim(),
    quality_tag: String(tradeQualityFilters.value.quality_tag || "").trim(),
    limit,
    offset: Math.max(0, Number(tradeQualityFilters.value.offset || 0))
  };
}

function tradeQualityBacktestPackageLimit(strategyLine = tradeQualityFilters.value.strategy_line) {
  return strategyLine && strategyLine !== "all" ? 10 : 30;
}

function tradeQualityBacktestMaterializeLimit() {
  return Math.min(500, Math.max(50, Number(tradeQualityFilters.value.limit || 80)));
}

function tradeQualityBacktestPackageBlocked(pkg) {
  return !pkg || pkg.sample_status === "metrics_only_no_trade_samples" || !pkg.has_shadow_orders;
}

function chooseTradeQualityBacktestPackage(rows, requestedKey = "") {
  if (!Array.isArray(rows) || !rows.length) return null;
  const requested = requestedKey ? rows.find((row) => row.package_key === requestedKey) : null;
  if (requested) return requested;
  return rows.find((row) => row.sample_status === "materialized")
    || rows.find((row) => row.sample_status === "ready_to_materialize")
    || rows[0]
    || null;
}

function tradeQualityBacktestPackageLabel(pkg) {
  if (!pkg) return "select backtest package";
  const metric = pkg.metrics || {};
  const exp = pkg.experiment_id_short || String(pkg.experiment_id || "").slice(0, 12) || "-";
  const param = String(pkg.parameter_set_id || "-").slice(0, 16);
  return `#${pkg.rank || "-"} ${pkg.strategy_line} · ${param} · exp ${exp} · PF ${metric.profit_factor ?? "-"} · R ${money(metric.total_R)} · trades ${metric.trade_count || 0} · shadow ${pkg.shadow_order_count || 0} · ${pkg.sample_status || "candidate"}`;
}

function applyTradeQualityBacktestPackage(pkg, { refresh = true } = {}) {
  if (!pkg) return;
  tradeQualityFilters.value.package_key = pkg.package_key || "";
  tradeQualityFilters.value.experiment_id = pkg.experiment_id || "";
  tradeQualityFilters.value.parameter_set_id = pkg.parameter_set_id || "";
  tradeQualityFilters.value.backtest_package_strategy_line = pkg.strategy_line || "";
  tradeQualityFilters.value.offset = 0;
  if (refresh) refreshTradeQuality();
}

function selectTradeQualityBacktestPackageFromFilter() {
  const packageKey = String(tradeQualityFilters.value.package_key || "");
  const pkg = tradeQualityPackageRows.value.find((row) => row.package_key === packageKey);
  applyTradeQualityBacktestPackage(pkg);
}

function tradeQualitySelectedBacktestParams() {
  const selected = selectedTradeQualityPackage.value;
  const params = tradeQualityParams();
  const experimentId = selected?.experiment_id || params.experiment_id;
  const parameterSetId = selected?.parameter_set_id || params.parameter_set_id;
  const packageStrategy = selected?.strategy_line || params.backtest_package_strategy_line;
  return {
    experiment_id: experimentId,
    strategy_line: packageStrategy,
    parameter_set_id: parameterSetId,
    package_key: selected?.package_key || params.package_key,
    symbol: params.symbol,
    side: params.side,
    exit_reason: params.exit_reason,
    root_cause: params.root_cause,
    entry_quality_label: params.entry_quality_label,
    entry_context_v3_label: params.entry_context_v3_label,
    limit: params.limit,
    offset: params.offset
  };
}

async function refreshTradeQualityShell(seq) {
  tradeQualityLoading.value = true;
  tradeQualityLazyMessage.value = "";
  try {
    const params = tradeQualityParams();
    if (params.source === "backtest_p21_v2") {
      tradeQualityLazyMessage.value = "loading backtest quality package candidates";
      const packages = await api.backtestP21V2QualityPackages({
        mode: "leaderboard_candidates",
        strategy_line: params.strategy_line,
        limit: tradeQualityBacktestPackageLimit(params.strategy_line)
      });
      if (seq !== tradeQualityRequestSeq.value || activePage.value !== "trade-quality") return false;
      tradeQualityPackages.value = packages || { packages: [] };
      tradeQualitySyncStatus.value = {
        stale: false,
        last_synced_at: packages?.generated_at || "",
        next_recommended_action: "select_or_materialize_backtest_package"
      };
      const rows = packages?.packages || [];
      const selected = chooseTradeQualityBacktestPackage(rows, params.package_key);
      if (selected && selected.package_key !== params.package_key) {
        applyTradeQualityBacktestPackage(selected, { refresh: false });
      }
      if (!selected) {
        tradeQuality.value = { ...tradeQuality.value, summary: {}, phenomena: [], total: 0, samples: [], aggregates: [], replay_ledger: [] };
        tradeQualityLazyMessage.value = "no backtest package candidates yet";
        return true;
      }
      const summaryParams = tradeQualitySelectedBacktestParams();
      const summary = await api.backtestP21V2QualitySummary(summaryParams);
      if (seq !== tradeQualityRequestSeq.value || activePage.value !== "trade-quality") return false;
      tradeQuality.value = {
        ...tradeQuality.value,
        summary: summary.summary || {},
        phenomena: summary.summary?.phenomena || [],
        total: summary.total || 0
      };
      tradeQualityLazyMessage.value = tradeQualityBacktestPackageBlocked(selected)
        ? "selected package has metrics but no shadow orders; cannot run 1m replay"
        : selected.materialized
        ? "backtest summary loaded; loading details in background"
        : "selected backtest package is not materialized; run dry-run or bounded materialize";
      return true;
    }
    if (params.source === "current_paper") {
      tradeQualityLazyMessage.value = "syncing current paper closed trades";
      const sync = await api.tradeQualityDiagnosticsSyncRun(null, "current_paper");
      if (seq !== tradeQualityRequestSeq.value || activePage.value !== "trade-quality") return false;
      tradeQualityLazyMessage.value = `current paper synced: ${sync.source_counts?.current_paper || sync.candidate_samples || 0} closed trades`;
    }
    const [summary, packages, syncStatus] = await Promise.all([
      api.tradeQualityDiagnosticsSummary(params),
      api.tradeQualityDiagnosticsArchivePackages(),
      api.tradeQualityDiagnosticsSyncStatus()
    ]);
    if (seq !== tradeQualityRequestSeq.value || activePage.value !== "trade-quality") return false;
    tradeQualityPackages.value = packages || { packages: [] };
    tradeQualitySyncStatus.value = syncStatus || {};
    tradeQuality.value = {
      ...tradeQuality.value,
      summary: summary.summary || {},
      phenomena: summary.summary?.phenomena || [],
      total: summary.total || tradeQuality.value?.total || 0
    };
    tradeQualityLazyMessage.value = "summary loaded; loading details in background";
    return true;
  } catch (exc) {
    tradeQualityLazyMessage.value = `Trade Quality shell load failed: ${exc.message}`;
    if (!tradeQuality.value?.summary || !Object.keys(tradeQuality.value.summary).length) error.value = exc.message;
    return false;
  } finally {
    tradeQualityLoading.value = false;
  }
}

async function refreshTradeQualityDetails(seq = tradeQualityRequestSeq.value) {
  tradeQualityDetailsLoading.value = true;
  try {
    const params = tradeQualityParams();
    if (params.source === "backtest_p21_v2") {
      const selectedParams = tradeQualitySelectedBacktestParams();
      if (!selectedParams.experiment_id || !selectedParams.parameter_set_id || !selectedParams.strategy_line) {
        tradeQuality.value = { ...tradeQuality.value, samples: [], aggregates: [], replay_ledger: [], total: 0 };
        tradeQualityLazyMessage.value = "select a backtest package before loading details";
        return;
      }
      const [samples, aggregates] = await Promise.all([
        api.backtestP21V2QualitySamples(selectedParams),
        api.backtestP21V2QualityAggregates(selectedParams)
      ]);
      if (seq !== tradeQualityRequestSeq.value || activePage.value !== "trade-quality") return;
      tradeQuality.value = {
        ...tradeQuality.value,
        samples: samples.samples || [],
        total: samples.total || tradeQuality.value?.total || 0,
        aggregates: aggregates.aggregates || [],
        replay_ledger: []
      };
      if ((samples.total || 0) > 0) tradeQualityLazyMessage.value = "";
      return;
    }
    const [samples, aggregates, replayLedger] = await Promise.all([
      api.tradeQualityDiagnosticsSamples(params),
      api.tradeQualityDiagnosticsAggregates(params),
      api.tradeQualityDiagnosticsReplayLedger(100)
    ]);
    if (seq !== tradeQualityRequestSeq.value || activePage.value !== "trade-quality") return;
    tradeQuality.value = {
      ...tradeQuality.value,
      samples: samples.samples || [],
      total: samples.total || tradeQuality.value?.total || 0,
      aggregates: aggregates.aggregates || [],
      replay_ledger: replayLedger.ledger || []
    };
    tradeQualityLazyMessage.value = "";
  } catch (exc) {
    tradeQualityLazyMessage.value = `Trade Quality details load failed; cached data kept: ${exc.message}`;
  } finally {
    if (seq === tradeQualityRequestSeq.value) tradeQualityDetailsLoading.value = false;
  }
}

async function refreshTradeQualityV4() {
  try {
    const params = {
      strategy_line: tradeQualityFilters.value.strategy_line,
      parameter_set_id: tradeQualityFilters.value.parameter_set_id,
      limit: 120
    };
    const [summary, evidence, deep, gates] = await Promise.allSettled([
      api.tradeQualityV4Summary(),
      api.tradeQualityV4Evidence(params),
      api.tradeQualityV4DeepRootCauses(params),
      api.tradeQualityV4GateCandidates(params)
    ]);
    tradeQualityV4.value = {
      summary: summary.status === "fulfilled" ? summary.value : tradeQualityV4.value.summary || {},
      evidence: evidence.status === "fulfilled" ? evidence.value : tradeQualityV4.value.evidence || { rows: [], coverage: [] },
      deep: deep.status === "fulfilled" ? deep.value : tradeQualityV4.value.deep || { rollups: [] },
      gates: gates.status === "fulfilled" ? gates.value : tradeQualityV4.value.gates || { candidates: [] }
    };
  } catch (exc) {
    tradeQualityV4Message.value = `V4 evidence load failed: ${exc.message}`;
  }
}

async function runTradeQualityV4Materialize() {
  if (tradeQualityV4Loading.value) return;
  tradeQualityV4Loading.value = true;
  tradeQualityV4Message.value = "materializing V4 evidence";
  try {
    const params = {};
    if (tradeQualityFilters.value.strategy_line && tradeQualityFilters.value.strategy_line !== "all") {
      params.strategy_line = tradeQualityFilters.value.strategy_line;
    }
    const result = await api.tradeQualityV4Materialize(params);
    tradeQualityV4Message.value = `V4 materialized: ${result.materialized_features || 0} features`;
    await refreshTradeQualityV4();
  } catch (exc) {
    tradeQualityV4Message.value = `V4 materialize failed: ${exc.message}`;
  } finally {
    tradeQualityV4Loading.value = false;
  }
}

async function runTradeQualityV4GateCandidates() {
  if (tradeQualityV4Loading.value) return;
  tradeQualityV4Loading.value = true;
  tradeQualityV4Message.value = "generating V4 shadow gates";
  try {
    const result = await api.tradeQualityV4GateCandidatesGenerate({
      strategy_line: tradeQualityFilters.value.strategy_line,
      min_samples: 50,
      limit: 80
    });
    tradeQualityV4Message.value = `V4 shadow gates: ${result.candidate_count || 0}`;
    await refreshTradeQualityV4();
  } catch (exc) {
    tradeQualityV4Message.value = `V4 gate generation failed: ${exc.message}`;
  } finally {
    tradeQualityV4Loading.value = false;
  }
}

async function refreshTradeQualityV5() {
  try {
    const params = {
      strategy_line: tradeQualityFilters.value.strategy_line,
      parameter_set_id: tradeQualityFilters.value.parameter_set_id,
      root_cause: tradeQualityFilters.value.root_cause,
      limit: 120
    };
    const [summary, causal, gates, coverage] = await Promise.allSettled([
      api.tradeQualityV5Summary(),
      api.tradeQualityV5CausalFactors(params),
      api.tradeQualityV5GateCandidates(params),
      api.tradeQualityV5WriterCoverage()
    ]);
    tradeQualityV5.value = {
      summary: summary.status === "fulfilled" ? summary.value : tradeQualityV5.value.summary || {},
      causal: causal.status === "fulfilled" ? causal.value : tradeQualityV5.value.causal || { rows: [], rollups: [] },
      gates: gates.status === "fulfilled" ? gates.value : tradeQualityV5.value.gates || { candidates: [] },
      coverage: coverage.status === "fulfilled" ? coverage.value : tradeQualityV5.value.coverage || {}
    };
  } catch (exc) {
    tradeQualityV5Message.value = `V5 evidence load failed: ${exc.message}`;
  }
}

async function runTradeQualityV5Materialize() {
  if (tradeQualityV5Loading.value) return;
  tradeQualityV5Loading.value = true;
  tradeQualityV5Message.value = "materializing V5 causal factors";
  try {
    const params = {};
    if (tradeQualityFilters.value.strategy_line && tradeQualityFilters.value.strategy_line !== "all") {
      params.strategy_line = tradeQualityFilters.value.strategy_line;
    }
    const result = await api.tradeQualityV5Materialize(params);
    tradeQualityV5Message.value = `V5 materialized: ${result.materialized_causal_rows || 0} rows`;
    await refreshTradeQualityV5();
  } catch (exc) {
    tradeQualityV5Message.value = `V5 materialize failed: ${exc.message}`;
  } finally {
    tradeQualityV5Loading.value = false;
  }
}

async function runTradeQualityV5GateCandidates() {
  if (tradeQualityV5Loading.value) return;
  tradeQualityV5Loading.value = true;
  tradeQualityV5Message.value = "generating V5 shadow gates";
  try {
    const result = await api.tradeQualityV5GateCandidatesGenerate({
      strategy_line: tradeQualityFilters.value.strategy_line,
      min_samples: 50,
      limit: 120
    });
    tradeQualityV5Message.value = `V5 shadow gates: ${result.candidate_count || 0}`;
    await refreshTradeQualityV5();
  } catch (exc) {
    tradeQualityV5Message.value = `V5 gate generation failed: ${exc.message}`;
  } finally {
    tradeQualityV5Loading.value = false;
  }
}

async function refreshTradeQuality() {
  const seq = tradeQualityRequestSeq.value + 1;
  tradeQualityRequestSeq.value = seq;
  const shellOk = await refreshTradeQualityShell(seq);
  if (shellOk) scheduleIdle(() => refreshTradeQualityDetails(seq));
  scheduleIdle(() => refreshTradeQualityV4());
  scheduleIdle(() => refreshTradeQualityV5());
}

async function refreshTradeQualityDetailsPage() {
  const seq = tradeQualityRequestSeq.value + 1;
  tradeQualityRequestSeq.value = seq;
  await refreshTradeQualityDetails(seq);
}

function tradeQualityPrevPage() {
  const limit = Number(tradeQualityFilters.value.limit || 200);
  tradeQualityFilters.value.offset = Math.max(0, Number(tradeQualityFilters.value.offset || 0) - limit);
  refreshTradeQualityDetailsPage();
}

function tradeQualityNextPage() {
  const limit = Number(tradeQualityFilters.value.limit || 200);
  tradeQualityFilters.value.offset = Number(tradeQualityFilters.value.offset || 0) + limit;
  refreshTradeQualityDetailsPage();
}

function selectTradeQualityRootCause(rootCause) {
  if (!rootCause) return;
  tradeQualityFilters.value.root_cause = rootCause;
  tradeQualityFilters.value.offset = 0;
  refreshTradeQuality();
}

function selectTradeQualityDimension(row) {
  if (!row) return;
  if (row.dimension === "symbol") {
    tradeQualityFilters.value.symbol = row.key || "";
  } else if (row.dimension === "side") {
    tradeQualityFilters.value.side = row.key || "all";
  } else {
    return;
  }
  tradeQualityFilters.value.offset = 0;
  refreshTradeQuality();
}

async function refreshTradeQualityIngestLedger() {
  try {
    const result = await api.tradeQualityIngestLedger(100);
    tradeQualityIngest.value = { ...tradeQualityIngest.value, ...result, latest: tradeQualityIngest.value.latest || {} };
  } catch (exc) {
    error.value = exc.message;
  }
}

async function runTradeQualityArchiveBackfill(dryRun = true) {
  tradeQualityIngestLoading.value = true;
  try {
    const result = dryRun
      ? await api.tradeQualityArchiveBackfillDryRun()
      : await api.tradeQualityArchiveBackfillRun();
    tradeQualityIngest.value = { ...tradeQualityIngest.value, latest: result };
    await refreshTradeQualityIngestLedger();
    await refreshTradeQuality();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityIngestLoading.value = false;
  }
}

async function refreshTradeQualityReplayLedger() {
  try {
    const result = await api.tradeQualityDiagnosticsReplayLedger(100);
    tradeQualityReplay.value = { ...tradeQualityReplay.value, ...result, latest: tradeQualityReplay.value.latest || {} };
  } catch (exc) {
    error.value = exc.message;
  }
}

async function runTradeQualityReplayBackfill(dryRun = true) {
  tradeQualityReplayLoading.value = true;
  try {
    const source = tradeQualityFilters.value.source || "all";
    const archiveId = source === "archive" ? String(tradeQualityFilters.value.archive_id || "").trim() : "";
    const result = dryRun
      ? await api.tradeQualityDiagnosticsReplayDryRun(50, source, archiveId)
      : await api.tradeQualityDiagnosticsReplayRun(50, source, archiveId);
    tradeQualityReplay.value = { ...tradeQualityReplay.value, latest: result };
    await refreshTradeQualityReplayLedger();
    await refreshTradeQuality();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityReplayLoading.value = false;
  }
}

async function runTradeQualityEntryFeatureBackfill(dryRun = true) {
  tradeQualityEntryFeatureLoading.value = true;
  try {
    const source = tradeQualityFilters.value.source || "all";
    const archiveId = source === "archive" ? String(tradeQualityFilters.value.archive_id || "").trim() : "";
    const result = dryRun
      ? await api.tradeQualityEntryFeaturesBackfillDryRun(100, source, archiveId)
      : await api.tradeQualityEntryFeaturesBackfillRun(100, source, archiveId);
    tradeQualityEntryFeatureLast.value = result;
    await refreshTradeQuality();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityEntryFeatureLoading.value = false;
  }
}

async function runTradeQualityEntryMicroBackfill(dryRun = true) {
  tradeQualityEntryMicroLoading.value = true;
  try {
    const source = tradeQualityFilters.value.source || "all";
    const archiveId = source === "archive" ? String(tradeQualityFilters.value.archive_id || "").trim() : "";
    const result = dryRun
      ? await api.tradeQualityEntryMicrostructureBackfillDryRun(100, source, archiveId)
      : await api.tradeQualityEntryMicrostructureBackfillRun(100, source, archiveId);
    tradeQualityEntryMicroLast.value = result;
    await refreshTradeQuality();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityEntryMicroLoading.value = false;
  }
}

async function runTradeQualityRefreshEnrich() {
  if (tradeQualityRefreshBusy.value) return;
  tradeQualityRefreshBusy.value = true;
  tradeQualityRefreshStage.value = "starting";
  error.value = "";
  try {
    const source = tradeQualityFilters.value.source || "current_paper";
    if (source === "backtest_p21_v2") {
      tradeQualityRefreshStage.value = "resolving backtest package";
      let seq = tradeQualityRequestSeq.value + 1;
      tradeQualityRequestSeq.value = seq;
      const shellOk = await refreshTradeQualityShell(seq);
      if (!shellOk) {
        tradeQualityRefreshLast.value = { status: "failed", source: "backtest_p21_v2", message: "failed to resolve selected package" };
        tradeQualityRefreshStage.value = "failed";
        return;
      }
      const selected = selectedTradeQualityPackage.value;
      if (!selected) {
        tradeQualityRefreshLast.value = { status: "blocked", source: "backtest_p21_v2", reason: "no_backtest_package" };
        tradeQualityRefreshStage.value = "blocked: no package";
        return;
      }
      if (tradeQualityBacktestPackageBlocked(selected)) {
        tradeQualityRefreshLast.value = {
          status: "blocked",
          source: "backtest_p21_v2",
          package_key: selected.package_key,
          sample_status_before: selected.sample_status,
          reason: "metrics_only_no_trade_samples",
          message: "this package has metrics but no shadow orders; cannot replay"
        };
        tradeQualityRefreshStage.value = "blocked: metrics only";
        tradeQualityLazyMessage.value = "selected package has metrics but no shadow orders; choose a ready/materialized package";
        return;
      }
      if (!selected.materialized) {
        tradeQualityRefreshStage.value = "bounded materialize + 1m replay";
        const result = await api.backtestP21V2QualityMaterialize({
          experiment_id: selected.experiment_id,
          strategy_line: selected.strategy_line,
          parameter_set_id: selected.parameter_set_id,
          top_n: 1,
          limit: tradeQualityBacktestMaterializeLimit(),
          dry_run: false,
          force: false
        });
        tradeQualityRefreshLast.value = {
          ...result,
          status: "ok",
          source: "backtest_p21_v2",
          package_key: selected.package_key,
          sample_status_before: selected.sample_status,
          bounded_limit: tradeQualityBacktestMaterializeLimit(),
          message: "materialized selected package and reloaded diagnostics"
        };
      } else {
        tradeQualityRefreshLast.value = {
          status: "ok",
          source: "backtest_p21_v2",
          package_key: selected.package_key,
          sample_status_before: selected.sample_status,
          materialized_count: selected.materialized_sample_count || selected.sample_count || 0,
          message: "selected package already materialized; reloaded diagnostics"
        };
      }
      tradeQualityRefreshStage.value = "reloading diagnostics";
      seq = tradeQualityRequestSeq.value + 1;
      tradeQualityRequestSeq.value = seq;
      const reloadOk = await refreshTradeQualityShell(seq);
      if (reloadOk) await refreshTradeQualityDetails(seq);
      tradeQualityRefreshStage.value = "completed";
      return;
    }
    const archiveId = source === "archive" ? String(tradeQualityFilters.value.archive_id || "").trim() : "";
    tradeQualityRefreshStage.value = "sync / replay / enrich";
    const result = await api.tradeQualityDiagnosticsRefreshEnrich(
      Math.min(500, Math.max(20, Number(tradeQualityFilters.value.limit || 100))),
      source,
      archiveId,
      false,
      false
    );
    tradeQualityRefreshLast.value = result;
    const failed = (result?.stages || []).find((stage) => stage.status === "failed");
    tradeQualityRefreshStage.value = failed ? `failed: ${failed.stage}` : "completed";
    await refreshTradeQualityReplayLedger();
    await refreshTradeQuality();
  } catch (exc) {
    tradeQualityRefreshStage.value = "failed";
    error.value = exc.message;
  } finally {
    tradeQualityRefreshBusy.value = false;
  }
}

async function runTradeQualityBacktestMaterialize(dryRun = true) {
  if (tradeQualityRefreshBusy.value) return;
  const selected = selectedTradeQualityPackage.value;
  if (!selected) {
    tradeQualityLazyMessage.value = "select a backtest package first";
    return;
  }
  tradeQualityRefreshBusy.value = true;
  tradeQualityRefreshStage.value = dryRun ? "backtest materialize dry-run" : "backtest materializing";
  error.value = "";
  try {
    const result = await api.backtestP21V2QualityMaterialize({
      experiment_id: selected.experiment_id,
      strategy_line: selected.strategy_line,
      parameter_set_id: selected.parameter_set_id,
      top_n: 1,
      limit: tradeQualityBacktestMaterializeLimit(),
      dry_run: dryRun,
      force: false
    });
    tradeQualityRefreshLast.value = { ...result, status: "ok" };
    tradeQualityRefreshStage.value = dryRun
      ? `dry-run selected ${result.selected_order_count || 0}`
      : `materialized ${result.materialized_count || 0}`;
    if (!dryRun) await refreshTradeQuality();
  } catch (exc) {
    tradeQualityRefreshStage.value = "failed";
    error.value = exc.message;
  } finally {
    tradeQualityRefreshBusy.value = false;
  }
}

function selectTradeQualityEntryLabel(label) {
  if (!label) return;
  tradeQualityFilters.value.entry_quality_label = label;
  tradeQualityFilters.value.offset = 0;
  refreshTradeQuality();
}

function selectTradeQualityEntryV2Label(label) {
  if (!label) return;
  tradeQualityFilters.value.entry_quality_v2_label = label;
  tradeQualityFilters.value.offset = 0;
  refreshTradeQuality();
}

function selectTradeQualityMarketContextLabel(label) {
  if (!label) return;
  tradeQualityFilters.value.market_context_label = label;
  tradeQualityFilters.value.offset = 0;
  refreshTradeQuality();
}

function selectTradeQualityEntryV3Label(label) {
  if (!label) return;
  tradeQualityFilters.value.entry_context_v3_label = label;
  tradeQualityFilters.value.offset = 0;
  refreshTradeQuality();
}

function selectTradeQualitySample(row) {
  tradeQualitySelectedSample.value = row;
}

async function refreshTradeQualityRules() {
  tradeQualityRulesLoading.value = true;
  try {
    const params = {
      ...tradeQualityRuleFilters.value,
      symbol: String(tradeQualityRuleFilters.value.symbol || "").trim().toUpperCase(),
      limit: 300
    };
    tradeQualityRules.value = await api.tradeQualityRecommendationRules(params);
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityRulesLoading.value = false;
  }
}

async function rebuildTradeQualityRules() {
  tradeQualityRulesLoading.value = true;
  try {
    await api.tradeQualityRecommendationRulesRebuild();
    await refreshTradeQualityRules();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityRulesLoading.value = false;
  }
}

async function refreshTradeQualityValidation() {
  tradeQualityValidationLoading.value = true;
  try {
    const params = {
      sample_source: "live",
      rule_type: tradeQualityRuleFilters.value.rule_type,
      strategy_line: tradeQualityRuleFilters.value.strategy_line,
      symbol: String(tradeQualityRuleFilters.value.symbol || "").trim().toUpperCase(),
      limit: 500
    };
    tradeQualityValidation.value = await api.tradeQualityRecommendationValidation(params);
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityValidationLoading.value = false;
  }
}

async function refreshTradeQualityPromotions() {
  tradeQualityPromotionLoading.value = true;
  try {
    tradeQualityPromotions.value = await api.tradeQualityRecommendationPromotions({ limit: 200 });
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityPromotionLoading.value = false;
  }
}

function promotionRequestForRule(rule) {
  return {
    rule_id: rule.rule_id,
    profile: tradeQualityPromotionDraft.value.profile,
    strategy_line: tradeQualityPromotionDraft.value.strategy_line === "all"
      ? (rule.strategy_line || null)
      : tradeQualityPromotionDraft.value.strategy_line,
    mode: tradeQualityPromotionDraft.value.mode,
    reason: "manual_review_ui"
  };
}

async function dryRunTradeQualityPromotion(rule) {
  tradeQualityPromotionLoading.value = true;
  try {
    tradeQualityPromotionPreview.value = await api.tradeQualityRecommendationPromotionDryRun(promotionRequestForRule(rule));
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityPromotionLoading.value = false;
  }
}

async function applyTradeQualityPromotion(rule) {
  tradeQualityPromotionLoading.value = true;
  try {
    tradeQualityPromotionPreview.value = await api.tradeQualityRecommendationPromotionApply(promotionRequestForRule(rule));
    await refreshTradeQualityPromotions();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityPromotionLoading.value = false;
  }
}

async function disableTradeQualityPromotion(row) {
  tradeQualityPromotionLoading.value = true;
  try {
    await api.tradeQualityRecommendationPromotionDisable({ promotion_id: row.promotion_id, reason: "manual_disable_ui" });
    await refreshTradeQualityPromotions();
  } catch (exc) {
    error.value = exc.message;
  } finally {
    tradeQualityPromotionLoading.value = false;
  }
}

function stopPaperAutoRefresh() {
  if (paperAutoRefreshTimer.value) {
    clearInterval(paperAutoRefreshTimer.value);
    paperAutoRefreshTimer.value = null;
  }
}

function startPaperAutoRefresh() {
  stopPaperAutoRefresh();
  refreshPaperRealtime();
  const now = new Date();
  paperNextRefreshAt.value = new Date(now.getTime() + 60_000).toLocaleTimeString();
  paperAutoRefreshTimer.value = setInterval(() => {
    if (activePage.value === "paper") refreshPaperRealtime();
  }, 60_000);
}

async function runAllOnce() {
  if (!canRunOnce.value) {
    activePage.value = "pipeline";
    pipelineLog(`RUN_ONCE blocked: ${runOnceDisabledReason.value || pipelineRunControls.value?.disabled_reason || "run disabled"}`, "warn");
    startPipelinePolling();
    return;
  }
  loading.value = true;
  activePage.value = "pipeline";
  pipelineUi.value.mode = "once";
  pipelineUi.value.overall = 0;
  pipelineLog(`RUN_ONCE submitted · ${selectedPipelineLineLabels.value}`, "ok");
  try {
    const result = await api.pipelineRun({ lines: selectedPipelineLines.value, mode: "once" });
    log(`RUN_ONCE ${result.status || "submitted"}`);
    pipelineLog(`RUN_ONCE ${result.status || "submitted"} pid=${result.pid || "-"}`, "ok");
    startPipelinePolling();
    await refreshAll();
  } catch (exc) {
    pipelineLog(`RUN_ONCE blocked: ${exc.message}`, "warn");
    startPipelinePolling();
    await refreshAll();
  } finally {
    loading.value = false;
  }
}

async function runFullChainCycle() {
  if (!canStartCycle.value) {
    activePage.value = "pipeline";
    pipelineLog(`RUN_CYCLE blocked: ${runCycleDisabledReason.value || pipelineRunControls.value?.disabled_reason || "run disabled"}`, "warn");
    startPipelinePolling();
    return;
  }
  loading.value = true;
  activePage.value = "pipeline";
  pipelineUi.value.mode = "cycle";
  pipelineUi.value.overall = 0;
  pipelineLog(`RUN_CYCLE submitted · ${selectedPipelineLineLabels.value}`, "ok");
  try {
    const interval = Number(config.value?.strategy_pipeline?.interval_sec || 60);
    const result = await api.pipelineRun({ lines: selectedPipelineLines.value, mode: "interval", interval_sec: interval });
    const cooldown = result.post_run_cooldown_sec || result.effective_interval_sec || interval;
    log(`FULL_CHAIN_CYCLE cooldown=${cooldown}s ${result.status || "started"}`);
    pipelineLog(`RUN_CYCLE ${result.status || "started"} cooldown=${cooldown}s pid=${result.pid || "-"}`, "ok");
    startPipelinePolling();
    await refreshAll();
  } catch (exc) {
    pipelineLog(`RUN_CYCLE blocked: ${exc.message}`, "warn");
    startPipelinePolling();
    await refreshAll();
  } finally {
    loading.value = false;
  }
}

async function stopPipelineRun() {
  if (!canStopPipeline.value) return;
  loading.value = true;
  activePage.value = "pipeline";
  pipelineLog("STOP requested", "warn");
  try {
    const result = await api.pipelineStop();
    pipelineLog(`STOP ${result.status || "submitted"} pid=${result.pid || "-"}`, "warn");
    pipelineUi.value.mode = "idle";
    await refreshAll();
  } catch (exc) {
    pipelineLog(`STOP failed: ${exc.message}`, "warn");
    await refreshAll();
  } finally {
    loading.value = false;
  }
}

async function toggleFullChainCycle() {
  if (isPipelineRunning.value) {
    await stopPipelineRun();
    return;
  }
  await runFullChainCycle();
}

async function sendFeishuMockSignals() {
  const result = await api.feishuSendTradePlans(true, true);
  log(`FEISHU mock cards selected=${JSON.stringify(result.selected)}`);
  await refreshAll();
}

async function sendFeishuCurrentSignals() {
  const result = await api.feishuSendTradePlans(false, false);
  log(`FEISHU current selected=${JSON.stringify(result.selected)}`);
  await refreshAll();
}

async function startRuntime() {
  const result = await api.runtimeStart();
  runtime.value = result;
  pipelineLog("runtime start requested", "ok");
  await refreshAll();
}

async function restartRuntime() {
  const result = await api.runtimeRestart();
  runtime.value = result;
  pipelineLog("runtime restart requested", "warn");
  await refreshAll();
}

async function openPaperDetail(line, symbol, row = null) {
  if (!symbol) return;
  detailModal.value = { loading: true, line, symbol, row, payload: null };
  const payload = await api.paperDetail(line, symbol);
  detailModal.value = { loading: false, line, symbol, row, payload };
}

async function archiveResetPaperLine(line) {
  if (!line || line === "overview" || paperArchiveBusy.value) return;
  const profile = configProfiles.value?.active_profile || config.value?.active_profile || "custom";
  const message = `Archive and reset ${lineLabel(line)}? Open positions will be force closed before archive.`;
  if (!window.confirm(message)) return;
  paperArchiveBusy.value = line;
  paperArchiveMessage.value = "";
  try {
    const result = await api.paperArchiveReset({
      strategy_line: line,
      profile_name: profile,
      notes: `UI archive/reset ${new Date().toISOString()}`
    });
    paperArchiveMessage.value = `archived ${result.experiment?.experiment_id || ""}`;
    await refreshAll();
    if (activePage.value === "paper") await refreshPaperRealtime();
  } catch (exc) {
    paperArchiveMessage.value = `archive failed: ${exc.message}`;
  } finally {
    paperArchiveBusy.value = "";
  }
}

async function openRunAudit(runId) {
  if (!runId) return;
  auditCoreLoading.value = true;
  try {
    auditDetailsRequested.value = false;
    showAuditRawPayload.value = false;
    await loadAuditCore(runId);
  } catch (exc) {
    error.value = exc.message;
  } finally {
    auditCoreLoading.value = false;
  }
}

async function openLatestRunAudit() {
  const first = runAuditRuns.value?.[0]?.run_id;
  if (first) {
    await openRunAudit(first);
    return;
  }
  try {
    auditDetailsRequested.value = false;
    showAuditRawPayload.value = false;
    await loadAuditCore();
  } catch (exc) {
    error.value = exc.message;
  }
}

onMounted(() => {
  refreshAll();
  if (activePage.value === "paper") startPaperAutoRefresh();
  if (activePage.value === "trade-quality") scheduleIdle(() => refreshTradeQualityReplayLedger());
});

watch(activePage, (page) => {
  const seq = pageLoadSeq.value + 1;
  pageLoadSeq.value = seq;
  scheduleIdle(() => loadActivePageData(seq));
  if (page === "paper") {
    startPaperAutoRefresh();
  } else {
    stopPaperAutoRefresh();
  }
  if (page === "backtest-lab") {
    startBacktestLabDiscovery();
  } else {
    stopBacktestLabDiscovery();
  }
  if (page === "trade-quality") {
    scheduleIdle(() => refreshTradeQualityIngestLedger());
    scheduleIdle(() => refreshTradeQualityRules());
    scheduleIdle(() => refreshTradeQualityValidation());
    scheduleIdle(() => refreshTradeQualityPromotions());
  } else {
    tradeQualityRequestSeq.value += 1;
    tradeQualityLoading.value = false;
    tradeQualityDetailsLoading.value = false;
  }
});

watch(paperTab, () => {
  if (activePage.value === "paper") refreshPaperRealtime();
});

onBeforeUnmount(() => {
  stopPaperAutoRefresh();
  stopBacktestLabPolling();
  stopBacktestLabDiscovery();
});
</script>

<template>
  <div class="app">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark">P13</div>
        <div class="brand-title">
          <strong>Trade Console</strong>
          <span>FastAPI / Vue3 Prototype</span>
        </div>
      </div>

      <nav class="nav">
        <div class="nav-group-label">Core</div>
        <button v-for="page in corePages" :key="page.key" :class="{ active: activePage === page.key }" @click="activePage = page.key">
          <component :is="page.icon" />
          <span>{{ page.label }}</span>
        </button>
        <div class="nav-group-label">Risk & Ops</div>
        <button v-for="page in riskPages" :key="page.key" :class="{ active: activePage === page.key }" @click="activePage = page.key">
          <component :is="page.icon" />
          <span>{{ page.label }}</span>
        </button>
      </nav>

      <div class="sidebar-footer">
        <div class="bot-status">
          <span>Micro Daemon</span>
          <span v-if="isRunningHealth(microHealth)" class="pulse-dot"></span>
          <span v-else class="tag warn">{{ microHealth?.status || "unknown" }}</span>
        </div>
        <div class="mini-metric"><span>Active targets</span><strong>{{ microHealth?.active_targets || micro?.active_targets || 0 }}</strong></div>
        <div class="mini-metric"><span>Heartbeat age</span><strong>{{ microHealth?.heartbeat_age_sec ?? "-" }}s</strong></div>
        <div class="mini-metric"><span>Runtime mode</span><strong>PAPER</strong></div>
      </div>
    </aside>

    <header class="topbar">
      <div class="topbar-left">
        <h1>{{ currentPage?.label }}</h1>
        <div class="identity">
          <span>/</span>
          <span>{{ tradeLines.without_micro?.run_id || "run_current" }}</span>
          <span>/</span>
          <span>{{ tradeLines.without_micro?.cycle_id || "cycle_current" }}</span>
          <span class="chip green">API healthy</span>
        </div>
      </div>
      <div class="topbar-right">
        <span class="chip yellow">latest refresh {{ health?.generated_at ? ageLabel(health.generated_at) : "-" }}</span>
        <span class="chip" :class="isCycleWaiting ? 'blue' : (pipeline?.job_running ? 'green' : 'yellow')">{{ pipelineStatusLabel }}</span>
        <div class="strategy-select compact" :class="{ disabled: isPipelineRunning || loading }">
          <button
            v-for="meta in pipelineStrategyMeta"
            :key="meta.line"
            class="strategy-toggle"
            :class="{ active: selectedPipelineLines.includes(meta.line) }"
            :disabled="!pipelineTrulyIdle || loading"
            @click="togglePipelineLine(meta.line)"
          >
            {{ lineNames[meta.line] }}
          </button>
        </div>
        <button class="btn primary" :disabled="!canRunOnce" :title="runOnceDisabledReason" @click="runAllOnce"><Play />Run All Once</button>
        <button
          class="btn"
          :class="isPipelineRunning ? 'stop' : 'cycle'"
          :disabled="cycleToggleDisabled"
          :title="runCycleDisabledReason"
          @click="toggleFullChainCycle"
        >
          <Square v-if="isPipelineRunning" />
          <RefreshCw v-else />
          {{ cycleButtonText }}
        </button>
        <button class="btn" :disabled="loading" @click="refreshAll"><RefreshCw />Refresh</button>
      </div>
    </header>

    <main class="content">
      <div v-if="error" class="alert bad">{{ error }}</div>
      <div v-if="runOnceDisabledReason || runCycleDisabledReason" class="alert info">
        Pipeline controls: run once {{ canRunOnce ? "ready" : runOnceDisabledReason || "disabled" }};
        run cycle {{ canStartCycle ? "ready" : runCycleDisabledReason || "disabled" }}.
      </div>
      <div v-if="snapshotWarmup?.ready === false" class="alert warn">
        Snapshot warmup not ready · usable {{ snapshotWarmup?.usable_symbol_count ?? 0 }} / {{ snapshotWarmup?.min_usable_symbol_count ?? 3 }}
        · {{ (snapshotWarmup?.reason_codes || []).join(", ") || pipelineRunControls?.disabled_reason || "warming" }}
      </div>

      <section v-if="activePage === 'dashboard'">
        <div class="hero">
          <div>
            <h2>Runtime Trading Dashboard</h2>
            <p>系统当前处于三层确认模式：without / fast / full 并行对照，重点监控 trade plan 可执行性、micro daemon 鲜度、paper PnL 与审计阻断项。</p>
          </div>
          <div class="market-strip">
            <div class="ticker"><div class="sym"><span>BTCUSDT</span><span class="up">+1.82%</span></div><div class="px">104,820</div></div>
            <div class="ticker"><div class="sym"><span>ETHUSDT</span><span class="up">+0.74%</span></div><div class="px">3,246</div></div>
            <div class="ticker"><div class="sym"><span>SOLUSDT</span><span class="down">-0.38%</span></div><div class="px">184.2</div></div>
            <div class="ticker"><div class="sym"><span>BNBUSDT</span><span class="flat">+0.05%</span></div><div class="px">682.4</div></div>
          </div>
        </div>

        <article class="card panel sandbox-context-card">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Active Sandbox Context</div>
            <button class="btn small" @click="activePage = 'sandbox-lab'"><DatabaseZap />Open Lab</button>
          </div>
          <div class="panel-body">
            <div class="form-grid">
              <label>
                Sandbox Plan
                <select v-model="sandboxLabSelectedId" @change="selectSandboxPlan(sandboxLabSelectedId)">
                  <option value="">no active sandbox</option>
                  <option v-for="item in sandboxLabRows" :key="item.sandbox_id" :value="item.sandbox_id">
                    {{ item.strategy_line }} · {{ item.sandbox_id }}
                  </option>
                </select>
              </label>
              <div class="stat">
                <span>Context</span>
                <strong>{{ sandboxLabActive?.active?.strategy_line || "-" }}</strong>
                <small>{{ sandboxLabActive?.active_sandbox_id || "no sandbox selected" }}</small>
              </div>
            </div>
          </div>
        </article>

        <div class="grid-kpi">
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">API Status</span><span class="status-icon"><CheckCircle2 /></span></div>
            <div class="kpi-value up">{{ health?.status || "Healthy" }}</div>
            <div class="kpi-sub">FastAPI facade · websocket proxy ready</div>
            <div class="kpi-line"></div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Micro Daemon</span><span class="status-icon"><Bot /></span></div>
            <div class="kpi-value status-value"><span v-if="isRunningHealth(microHealth)" class="pulse-dot inline"></span>{{ microHealth?.status || micro?.status || "unknown" }}</div>
            <div class="kpi-sub">{{ microHealth?.active_targets || micro?.active_targets || 0 }} active targets · {{ healthText(microHealth) }}</div>
            <div class="kpi-line"></div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Pipeline</span><span class="status-icon"><Activity /></span></div>
            <div class="kpi-value flat">{{ isCycleWaiting ? "Waiting" : (pipeline?.job_running ? "Running" : "Stopped") }}</div>
            <div class="kpi-sub">{{ pipelineStatusLabel }} · cooldown {{ pipeline?.post_run_cooldown_sec || pipeline?.effective_interval_sec || config?.strategy_pipeline?.interval_sec || 60 }}s</div>
            <div class="kpi-line amber"></div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Snapshot Daemon</span><span class="status-icon"><Gauge /></span></div>
            <div class="kpi-value status-value"><span v-if="isRunningHealth(snapshotDaemonSummary)" class="pulse-dot inline"></span>{{ snapshotDaemonSummary?.daemon_status || snapshotDaemonSummary?.status || "unknown" }}</div>
            <div class="kpi-sub">heartbeat {{ secondsText(snapshotDaemonSummary?.heartbeat_age_sec) }} · stale {{ snapshotFreshnessCounts?.stale_blocked || 0 }}</div>
            <div class="kpi-line"></div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Snapshot Warmup</span><span class="status-icon"><ShieldCheck /></span></div>
            <div class="kpi-value status-value">{{ snapshotWarmup?.status || "unknown" }}</div>
            <div class="kpi-sub">usable {{ snapshotWarmup?.usable_symbol_count ?? 0 }} / min {{ snapshotWarmup?.min_usable_symbol_count ?? 3 }}</div>
            <div class="kpi-line" :class="snapshotWarmup?.ready ? '' : 'amber'"></div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Paper Daemon</span><span class="status-icon"><WalletCards /></span></div>
            <div class="kpi-value status-value"><span v-if="isRunningHealth(paperHealth)" class="pulse-dot inline"></span>{{ paperHealth?.status || paperStatus?.status || "unknown" }}</div>
            <div class="kpi-sub">1m matching · {{ healthText(paperHealth) }}</div>
            <div class="kpi-line orange"></div>
          </article>
        </div>

        <div class="dashboard-grid">
          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Trade Plan Summary</div>
              <span class="tag good">freshness monitored</span>
            </div>
            <div class="panel-body">
              <div class="plan-grid">
                <div v-for="row in planSummary" :key="row.line" class="plan-box">
                  <div class="plan-name">{{ row.line }}</div>
                  <div class="plan-num">{{ row.count }}</div>
                  <div class="plan-meta"><span>executable</span><b>{{ row.executable }}</b></div>
                </div>
              </div>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Paper PnL Summary</div>
              <span class="tag blue">worker on</span>
            </div>
            <div class="panel-body">
              <div class="bars">
                <div v-for="line in strategyLineOrder" :key="line" class="bar-row">
                  <div class="bar-label">{{ line.replace('_micro', '') }}</div>
                  <div class="bar-track"><div class="bar-fill" :class="{ green: line === 'micro_fast', orange: line === 'micro_full' }" :style="{ width: `${Math.min(90, 35 + Math.abs(Number(paperStats[line]?.net_pnl || 0)))}%` }"></div></div>
                  <div class="bar-value" :class="Number(paperStats[line]?.net_pnl || 0) >= 0 ? 'up' : 'down'">{{ money(paperStats[line]?.net_pnl) }}</div>
                </div>
              </div>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Notifications</div>
              <span class="tag" :class="recentFailures ? 'warn' : 'good'">{{ recentFailures }} failed</span>
            </div>
            <div class="panel-body">
              <div class="notice-list">
                <div class="notice"><span>Feishu configured</span><span class="tag" :class="feishu.configured ? 'good' : 'warn'">{{ feishu.configured ? 'yes' : 'no' }}</span></div>
                <div class="notice"><span>Latest delivery</span><span class="tag" :class="statusClass(latestDelivery?.status)">{{ latestDelivery?.status || 'none' }}</span></div>
                <div class="notice"><span>Recent failures</span><span class="tag warn">{{ recentFailures }}</span></div>
              </div>
            </div>
          </article>
        </div>

        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Freshness Monitor</div>
              <span class="tag good">stream aligned</span>
            </div>
            <div class="panel-body">
              <table>
                <thead><tr><th>Source</th><th>Age</th><th>Status</th><th>Path</th></tr></thead>
                <tbody>
                  <tr v-for="row in freshnessRows" :key="row.source">
                    <td>{{ row.source }}</td>
                    <td>{{ row.age }}</td>
                    <td><span class="tag" :class="statusClass(row.status)">{{ row.status }}</span></td>
                    <td class="path">{{ row.path }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Latest Audit</div>
              <span class="tag warn">warning</span>
            </div>
            <div class="panel-body">
              <div class="audit-row"><div class="audit-code">P1/P2</div><div class="audit-name">Universe and snapshot contract</div><span class="tag good">pass</span></div>
              <div class="audit-row"><div class="audit-code">P3</div><div class="audit-name">Micro signal usability gate</div><span class="tag good">pass</span></div>
              <div class="audit-row"><div class="audit-code">P10</div><div class="audit-name">ABC line isolation</div><span class="tag warn">warn</span></div>
            </div>
          </article>
        </div>

        <div class="bottom-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Candidate Heat</div><span class="tag blue">top movers</span></div>
            <div class="panel-body heatmap">
              <div v-for="item in ['OPG','HYPE','WIF','ENA','SUI','SOL','BNB','DOGE','PEPE','ETH']" :key="item" class="heat-cell" :class="item.length % 3 === 0 ? 'good' : item.length % 2 === 0 ? 'hot' : 'bad'">
                <div class="heat-symbol">{{ item }}</div>
                <div class="heat-score">{{ 40 + item.charCodeAt(0) % 45 }}</div>
              </div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Execution Risk</div><span class="tag good">normal</span></div>
            <div class="panel-body">
              <div class="meter-ring"><div><strong>41</strong><span>risk score</span></div></div>
              <div class="risk-row"><span>Rate budget</span><strong class="up">OK</strong></div>
              <div class="risk-row"><span>Spread / slippage</span><strong class="flat">Medium</strong></div>
              <div class="risk-row"><span>Micro alignment</span><strong class="up">Fast pass</strong></div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Runtime Logs</div><span class="tag blue">tail</span></div>
            <div class="panel-body log-list">
              <div v-for="line in runtimeLog" :key="line" class="log-line">{{ line }}</div>
              <div v-if="!runtimeLog.length" class="log-line">waiting for runtime events...</div>
            </div>
          </article>
        </div>
        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Strategy Experiment History</div><span class="tag blue">{{ paperExperimentRows.length }} archived</span></div>
          <div class="panel-body">
            <table><thead><tr><th>策略</th><th>实验</th><th>参数</th><th>归档时间</th><th>订单</th><th>胜率</th><th>净盈亏</th><th>强平</th></tr></thead><tbody>
              <tr v-for="row in paperExperimentRows.slice(0, 20)" :key="row.experiment_id">
                <td>{{ lineLabel(row.strategy_line) }}</td><td>{{ row.experiment_id }}</td><td>{{ row.profile_name || '-' }}</td><td>{{ row.archived_at || '-' }}</td><td>{{ row.stats_before_reset?.total_orders || 0 }}</td><td>{{ money(row.stats_before_reset?.win_rate) }}%</td><td :class="pnlClass(row.stats_before_reset?.net_pnl_usdt)">{{ money(row.stats_before_reset?.net_pnl_usdt) }}</td><td>{{ row.forced_closed_positions || 0 }}</td>
              </tr>
              <tr v-if="!paperExperimentRows.length"><td colspan="8" class="empty-state">No archived experiment yet.</td></tr>
            </tbody></table>
          </div>
        </article>
      </section>

      <section v-else-if="activePage === 'config'">
        <PageHeader title="Config" subtitle="Strategy, runtime, paper and notification parameters. Updates go through the FastAPI whitelist contract." />
        <article class="card panel config-profile-panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Profile Presets</div>
            <span class="tag blue">active {{ configProfiles.active_profile || config.active_profile || 'custom' }}</span>
          </div>
          <div class="panel-body">
            <div class="profile-grid">
              <button
                v-for="profile in configProfiles.profiles || []"
                :key="profile.name"
                class="profile-card"
                :class="{ active: profile.name === (configProfiles.active_profile || config.active_profile) }"
                :disabled="Boolean(configSaving)"
                @click="applyConfigProfile(profile.name)"
              >
                <strong>{{ profile.name }}</strong>
                <span>{{ (profile.sections || []).join(' / ') }}</span>
              </button>
            </div>
            <div class="config-message-row">
              <span class="tag" :class="configMessage && configMessage.toLowerCase().includes('invalid') ? 'bad' : 'good'">{{ configMessage || 'ready' }}</span>
              <button class="btn" :disabled="Boolean(configSaving)" @click="reloadRuntimeConfig"><RefreshCw />Reload Config</button>
            </div>
          </div>
        </article>

        <article class="card panel config-governance-panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Config Governance</div>
            <span class="tag blue">{{ configFieldImpactSummary.field_count || 0 }} fields mapped</span>
          </div>
          <div class="panel-body">
            <div class="config-governance-grid">
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Executable Fields</span></div><div class="kpi-value">{{ configFieldImpactSummary.direct_executable_field_count || 0 }}</div><div class="kpi-sub">current direct impact</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Legacy / Disabled</span></div><div class="kpi-value warn">{{ configFieldImpactSummary.legacy_or_disabled_field_count || 0 }}</div><div class="kpi-sub">shielded from primary UI</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Primary Controls</span></div><div class="kpi-value">{{ configImpactCount('primary') }}</div><div class="kpi-sub">editable high-signal fields</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Minimal Hidden</span></div><div class="kpi-value">{{ configImpactCount('minimal_hidden') }}</div><div class="kpi-sub">STEP26.5 cleanup contract</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Advanced Fields</span></div><div class="kpi-value">{{ configImpactCount('advanced') }}</div><div class="kpi-sub">folded by default</div></article>
            </div>
            <div class="config-tab-bar">
              <button
                v-for="tab in configGovernanceTabs"
                :key="tab.key"
                class="segment-btn"
                :class="{ active: configActiveTab === tab.key }"
                @click="configActiveTab = tab.key"
              >
                {{ tab.label }}
              </button>
            </div>
            <div class="config-tab-hint">{{ (configGovernanceTabs.find((tab) => tab.key === configActiveTab) || {}).hint }}</div>
          </div>
        </article>

        <div class="line-grid config-line-grid">
          <article v-for="line in configLineSections" :key="line" class="card panel config-editor-card">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>{{ lineLabel(line) }} / {{ line }}</div>
              <div class="config-impact-mini">
                <span class="tag good">exec {{ configEffectiveCounts(line).direct_executable || 0 }}</span>
                <span class="tag blue">paper {{ configEffectiveCounts(line).paper_only || 0 }}</span>
                <span v-if="configEffectiveForLine(line).inherits_from" class="tag warn">inherits {{ configEffectiveForLine(line).inherits_from }}</span>
              </div>
              <button class="btn primary" :disabled="Boolean(configSaving)" @click="saveStrategyLineConfig(line)">
                <CheckCircle2 />Save
              </button>
            </div>
            <div class="panel-body">
              <div v-if="configLineWarning(line)" class="notice config-warning">
                <span>{{ configLineWarning(line) }}</span>
                <span class="tag warn">effective preview</span>
              </div>

              <div v-if="configActiveTab === 'strategy-runtime'" class="config-effective-card">
                <div class="market-now-header">
                  <strong>Effective Runtime Preview</strong>
                  <span class="tag blue">{{ configEffectiveForLine(line).live_executable_source?.base_plan || line }}</span>
                </div>
                <div class="config-preview-grid">
                  <div class="notice"><span>base plan</span><span class="tag blue">{{ configEffectiveForLine(line).live_executable_source?.base_plan || line }}</span></div>
                  <div class="notice"><span>evidence overlay</span><span class="tag blue">{{ configEffectiveForLine(line).live_executable_source?.evidence_overlay || "-" }}</span></div>
                  <div class="notice"><span>paper gate</span><span class="tag blue">{{ configEffectiveForLine(line).live_executable_source?.paper_gate || "paper config" }}</span></div>
                  <div class="notice"><span>backtest fields</span><span class="tag blue">{{ configEffectiveCounts(line).backtest || 0 }}</span></div>
                </div>
              </div>

              <div v-if="configActiveTab === 'strategy-runtime' && line !== 'without_micro'" class="micro-policy-controls">
                <label>
                  <span>Micro Consumption</span>
                  <select
                    :value="configDraftField(`line:${line}`, 'micro_consumption_policy', 'confirmed_only')"
                    @change="updateConfigDraftField(`line:${line}`, 'micro_consumption_policy', $event.target.value)"
                  >
                    <option v-for="policy in microConsumptionPolicies" :key="policy.value" :value="policy.value">
                      {{ policy.label }}
                    </option>
                  </select>
                </label>
                <label>
                  <span>Weak Min State</span>
                  <select
                    :value="configDraftField(`line:${line}`, 'weak_micro_min_state', 'ready')"
                    @change="updateConfigDraftField(`line:${line}`, 'weak_micro_min_state', $event.target.value)"
                  >
                    <option v-for="state in weakMicroMinStates" :key="state.value" :value="state.value">
                      {{ state.label }}
                    </option>
                  </select>
                </label>
                <label class="toggle-row">
                  <input
                    type="checkbox"
                    :checked="Boolean(configDraftField(`line:${line}`, 'allow_weak_micro_consumption', false))"
                    @change="updateConfigDraftBool(`line:${line}`, 'allow_weak_micro_consumption', $event)"
                  />
                  <span>Allow Weak Consumption</span>
                </label>
                <label class="toggle-row">
                  <input
                    type="checkbox"
                    :checked="Boolean(configDraftField(`line:${line}`, 'weak_micro_require_signal_usable', true))"
                    @change="updateConfigDraftBool(`line:${line}`, 'weak_micro_require_signal_usable', $event)"
                  />
                  <span>Require Signal Usable</span>
                </label>
                <label class="toggle-row">
                  <input
                    type="checkbox"
                    :checked="Boolean(configDraftField(`line:${line}`, 'weak_micro_require_direction_not_conflict', true))"
                    @change="updateConfigDraftBool(`line:${line}`, 'weak_micro_require_direction_not_conflict', $event)"
                  />
                  <span>Block Direction Conflict</span>
                </label>
              </div>

              <div v-if="configActiveTab === 'entry-executable'" class="config-impact-list">
                <div class="market-now-header">
                  <strong>Primary Executable Impact</strong>
                  <span class="tag good">{{ topLineImpactRows(line, { recommendation: 'primary' }, 8).length }} shown</span>
                </div>
                <div v-for="row in topLineImpactRows(line, { recommendation: 'primary' }, 8)" :key="`${line}:primary:${row.field_path}`" class="config-impact-row">
                  <span>{{ row.field_path }}</span>
                  <span class="tag" :class="configImpactTagClass(row)">{{ configImpactLabel(row) }}</span>
                </div>
              </div>

              <div v-if="configActiveTab === 'exit-rr'" class="market-now-controls">
                <div class="market-now-header">
                  <strong>Fast Exit TP Policy</strong>
                  <span class="tag blue">TP only · SL unchanged</span>
                </div>
                <div class="micro-policy-controls">
                  <label>
                    <span>Mode</span>
                    <select
                      :value="configDraftNestedField(`line:${line}`, ['tp_target_policy', 'mode'], 'structure')"
                      @change="updateConfigDraftNestedField(`line:${line}`, ['tp_target_policy', 'mode'], $event.target.value)"
                    >
                      <option v-for="mode in tpTargetPolicyModes" :key="mode.value" :value="mode.value">{{ mode.label }}</option>
                    </select>
                  </label>
                  <label>
                    <span>RR Basis</span>
                    <select
                      :value="configDraftNestedField(`line:${line}`, ['tp_target_policy', 'target_rr_basis'], 'gross')"
                      @change="updateConfigDraftNestedField(`line:${line}`, ['tp_target_policy', 'target_rr_basis'], $event.target.value)"
                    >
                      <option v-for="mode in tpTargetPolicyBasisModes" :key="mode.value" :value="mode.value">{{ mode.label }}</option>
                    </select>
                  </label>
                  <label>
                    <span>Sizing Basis</span>
                    <select
                      :value="configDraftNestedField(`line:${line}`, ['tp_target_policy', 'sizing_basis'], 'gross_stop')"
                      @change="updateConfigDraftNestedField(`line:${line}`, ['tp_target_policy', 'sizing_basis'], $event.target.value)"
                    >
                      <option v-for="mode in tpTargetPolicySizingBasisModes" :key="mode.value" :value="mode.value">{{ mode.label }}</option>
                    </select>
                  </label>
                  <label v-for="field in tpTargetPolicyNumberFields" :key="`${line}:tp-policy:${field.key}`">
                    <span>{{ field.label }}</span>
                    <input
                      type="number"
                      :step="field.step"
                      :value="configDraftNestedField(`line:${line}`, ['tp_target_policy', field.key], '')"
                      @change="updateConfigDraftNestedNumber(`line:${line}`, ['tp_target_policy', field.key], $event)"
                    />
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['tp_target_policy', 'require_market_room'], true))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['tp_target_policy', 'require_market_room'], $event)"
                    />
                    <span>Require Market Room</span>
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['tp_target_policy', 'allow_structure_runner'], false))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['tp_target_policy', 'allow_structure_runner'], $event)"
                    />
                    <span>Allow Structure Runner</span>
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['tp_target_policy', 'include_entry_fee'], true))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['tp_target_policy', 'include_entry_fee'], $event)"
                    />
                    <span>Include Entry Fee</span>
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['tp_target_policy', 'include_exit_fee'], true))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['tp_target_policy', 'include_exit_fee'], $event)"
                    />
                    <span>Include Exit Fee</span>
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['tp_target_policy', 'include_slippage_reserve'], false))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['tp_target_policy', 'include_slippage_reserve'], $event)"
                    />
                    <span>Include Slippage Reserve</span>
                  </label>
                </div>
              </div>

              <div v-if="configActiveTab === 'trade-gate'" class="notice config-warning">
                <span>V5 trade gate is applied before paper order. Legacy YAML gates are hidden from the minimal chain view and remain auditable in Advanced / Legacy.</span>
                <span class="tag good">V5 gate</span>
              </div>
              <div v-if="configActiveTab === 'advanced-legacy'" class="market-now-controls legacy-shield">
                <div class="market-now-header">
                  <strong>Trade Quality Gate</strong>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      disabled
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['trade_quality_gate', 'enabled'], false))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['trade_quality_gate', 'enabled'], $event)"
                    />
                    <span>Temporarily Disabled</span>
                  </label>
                </div>
                <div class="micro-policy-controls">
                  <label>
                    <span>Mode</span>
                    <select
                      disabled
                      :value="configDraftNestedField(`line:${line}`, ['trade_quality_gate', 'mode'], 'off')"
                      @change="updateConfigDraftNestedField(`line:${line}`, ['trade_quality_gate', 'mode'], $event.target.value)"
                    >
                      <option v-for="mode in tradeQualityGateModes" :key="mode.value" :value="mode.value">{{ mode.label }}</option>
                    </select>
                  </label>
                  <label v-for="field in tradeQualityGateNumberFields" :key="`${line}:tq:${field.key}`">
                    <span>{{ field.label }}</span>
                    <input
                      type="number"
                      disabled
                      :step="field.step"
                      :value="configDraftNestedField(`line:${line}`, ['trade_quality_gate', field.key], '')"
                      @change="updateConfigDraftNestedNumber(`line:${line}`, ['trade_quality_gate', field.key], $event)"
                    />
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      disabled
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['trade_quality_gate', 'signal_no_edge_wait_enabled'], true))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['trade_quality_gate', 'signal_no_edge_wait_enabled'], $event)"
                    />
                    <span>Signal No Edge Wait</span>
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      disabled
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['trade_quality_gate', 'side_specific_enabled'], true))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['trade_quality_gate', 'side_specific_enabled'], $event)"
                    />
                    <span>Side Specific</span>
                  </label>
                </div>
              </div>
              <div v-if="configActiveTab === 'advanced-legacy'" class="market-now-controls legacy-shield">
                <div class="market-now-header">
                  <strong>SL / TP Quality</strong>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      disabled
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['sl_tp_quality', 'enabled'], false))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['sl_tp_quality', 'enabled'], $event)"
                    />
                    <span>Temporarily Disabled</span>
                  </label>
                </div>
                <div class="micro-policy-controls">
                  <label>
                    <span>Mode</span>
                    <select
                      disabled
                      :value="configDraftNestedField(`line:${line}`, ['sl_tp_quality', 'mode'], 'off')"
                      @change="updateConfigDraftNestedField(`line:${line}`, ['sl_tp_quality', 'mode'], $event.target.value)"
                    >
                      <option v-for="mode in slTpQualityModes" :key="mode.value" :value="mode.value">{{ mode.label }}</option>
                    </select>
                  </label>
                  <label v-for="field in slTpQualityNumberFields" :key="`${line}:sltp:${field.key}`">
                    <span>{{ field.label }}</span>
                    <input
                      type="number"
                      disabled
                      :step="field.step"
                      :value="configDraftNestedField(`line:${line}`, ['sl_tp_quality', field.key], '')"
                      @change="updateConfigDraftNestedNumber(`line:${line}`, ['sl_tp_quality', field.key], $event)"
                    />
                  </label>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      disabled
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['sl_tp_quality', 'single_tp_only'], true))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['sl_tp_quality', 'single_tp_only'], $event)"
                    />
                    <span>Single TP Only</span>
                  </label>
                </div>
              </div>

              <div v-if="configActiveTab === 'entry-executable'" class="market-now-controls">
                <div class="market-now-header">
                  <strong>Market NOW Calibration</strong>
                  <label class="toggle-row">
                    <input
                      type="checkbox"
                      :checked="Boolean(configDraftNestedField(`line:${line}`, ['market_now_calibration', 'enabled'], false))"
                      @change="updateConfigDraftNestedBool(`line:${line}`, ['market_now_calibration', 'enabled'], $event)"
                    />
                    <span>Enabled</span>
                  </label>
                </div>
                <div class="market-now-side-grid">
                  <div v-for="side in marketNowSides" :key="`${line}:${side.key}`" class="market-now-side">
                    <div class="market-now-side-title">{{ side.label }}</div>
                    <label v-for="field in marketNowNumberFields" :key="`${line}:${side.key}:${field.key}`">
                      <span>{{ field.label }}</span>
                      <input
                        type="number"
                        step="0.01"
                        :value="configDraftNestedField(`line:${line}`, ['market_now_calibration', side.key, field.key], '')"
                        @change="updateConfigDraftNestedNumber(`line:${line}`, ['market_now_calibration', side.key, field.key], $event)"
                      />
                    </label>
                    <label class="toggle-row">
                      <input
                        type="checkbox"
                        :checked="Boolean(configDraftNestedField(`line:${line}`, ['market_now_calibration', side.key, 'allow_if_liquidity_missing'], false))"
                        @change="updateConfigDraftNestedBool(`line:${line}`, ['market_now_calibration', side.key, 'allow_if_liquidity_missing'], $event)"
                      />
                      <span>Allow Missing Liquidity</span>
                    </label>
                  </div>
                </div>
              </div>

              <div v-if="configActiveTab === 'advanced-legacy'" class="config-impact-list">
                <div class="market-now-header">
                  <strong>Advanced / Legacy Impact</strong>
                  <span class="tag warn">{{ configEffectiveCounts(line).legacy_or_disabled || 0 }} legacy</span>
                </div>
                <div v-for="row in topLineImpactRows(line, { recommendation: 'hide_legacy' }, 10)" :key="`${line}:legacy:${row.field_path}`" class="config-impact-row">
                  <span>{{ row.field_path }}</span>
                  <span class="tag" :class="configImpactTagClass(row)">{{ configImpactLabel(row) }}</span>
                </div>
              </div>
              <textarea v-if="configActiveTab === 'advanced-legacy'" v-model="configDrafts[`line:${line}`]" class="config-textarea" spellcheck="false"></textarea>
            </div>
          </article>
        </div>

        <div v-if="configActiveTab === 'strategy-runtime' || configActiveTab === 'advanced-legacy'" class="config-section-grid">
          <article v-for="section in visibleConfigSections" :key="section.key" class="card panel config-editor-card">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>{{ section.title }}</div>
              <button class="btn primary" :disabled="Boolean(configSaving)" @click="saveConfigSection(section)">
                <CheckCircle2 />Save
              </button>
            </div>
            <div class="panel-body">
              <textarea v-model="configDrafts[`section:${section.key}`]" class="config-textarea compact" spellcheck="false"></textarea>
            </div>
          </article>
        </div>
        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Paper Intent Inbox</div><span class="tag blue">{{ paperIntentRows.length }} intents</span></div>
            <div class="panel-body">
              <table><thead><tr><th>Line</th><th>Symbol</th><th>Side</th><th>Status</th><th>Source Run</th><th>Reason</th><th>Updated</th></tr></thead><tbody>
                <tr v-for="row in paperIntentRows.slice(0, 20)" :key="row.intent_id || row.source_plan_hash">
                  <td>{{ lineLabel(row.strategy_line) }}</td><td>{{ row.symbol }}</td><td>{{ sideLabel(row.side) }}</td><td>{{ row.status || '-' }}</td><td>{{ row.source_run_id || '-' }}</td><td>{{ skipReasonLabel(row.skip_reason || row.skip_detail?.raw_reason || '-') }}</td><td>{{ row.updated_at || row.consumed_at || row.created_at || '-' }}</td>
                </tr>
                <tr v-if="!paperIntentRows.length"><td colspan="7" class="empty-state">No paper intents yet.</td></tr>
              </tbody></table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Archive Reset Epoch</div><span class="tag warn">{{ paperEpochRows.length }} epochs</span></div>
            <div class="panel-body">
              <table><thead><tr><th>Line</th><th>Epoch</th><th>Experiment</th><th>Reset At</th><th>After Run</th></tr></thead><tbody>
                <tr v-for="row in paperEpochRows.slice(0, 20)" :key="row.reset_epoch_id">
                  <td>{{ lineLabel(row.strategy_line) }}</td><td>{{ row.reset_epoch_id || '-' }}</td><td>{{ row.experiment_id || '-' }}</td><td>{{ row.reset_at || '-' }}</td><td>{{ row.reset_after_run_id || '-' }}</td>
                </tr>
                <tr v-if="!paperEpochRows.length"><td colspan="5" class="empty-state">No archive reset epoch yet.</td></tr>
              </tbody></table>
            </div>
          </article>
        </div>
      </section>

      <section v-else-if="false">
        <PageHeader title="Config" subtitle="P11 YAML runtime settings. API 只允许白名单字段写回。" />
        <div class="wide-grid">
          <ConfigPanel title="Strategy Pipeline" :data="config.strategy_pipeline" />
          <ConfigPanel title="Feishu" :data="feishu" />
        </div>
        <div class="wide-grid">
          <ConfigPanel title="Paper Daemon" :data="config.paper" />
          <ConfigPanel title="Position Sizing" :data="config.position_sizing" />
          <ConfigPanel title="Liquidity" :data="config.market_entry_liquidity" />
        </div>
      </section>

      <section v-else-if="activePage === 'micro'">
        <PageHeader title="Micro Daemon" subtitle="常驻订单流采集进程，提供 fast/full signal state。" />
        <div class="grid-kpi">
          <article class="card kpi"><div class="kpi-label">State</div><div class="kpi-value status-value"><span v-if="isRunningHealth(microHealth)" class="pulse-dot inline"></span>{{ microHealth?.status || micro?.status || "unknown" }}</div><div class="kpi-sub">{{ healthText(microHealth) }}</div></article>
          <article class="card kpi"><div class="kpi-label">Targets</div><div class="kpi-value">{{ microHealth?.active_targets || micro?.active_targets || 0 }}</div><div class="kpi-sub">max active symbols</div></article>
          <article class="card kpi"><div class="kpi-label">Fast Ready</div><div class="kpi-value up">90s</div><div class="kpi-sub">default fast collect</div></article>
          <article class="card kpi"><div class="kpi-label">Full Ready</div><div class="kpi-value flat">900s</div><div class="kpi-sub">full signal collect</div></article>
        </div>
        <div class="card panel"><div class="panel-header"><div class="panel-title"><span class="accent"></span>Daemon Actions</div></div><div class="panel-body action-row"><button class="btn primary" @click="startRuntime"><Play />Start Runtime</button><button class="btn cycle" @click="restartRuntime"><RefreshCw />Restart Runtime</button><button class="btn" @click="api.microStop().then(refreshAll)"><Square />Stop Micro</button><button class="btn" @click="refreshAll"><RefreshCw />Refresh</button></div></div>
        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Training Ledger</div>
            <span class="tag blue">AI training sidecar</span>
          </div>
          <div class="panel-body">
            <div class="grid-kpi">
              <article class="card kpi"><div class="kpi-label">Latest Run</div><div class="kpi-value">{{ microTrainingSummary.run_id || microTrainingSummary.latest_run_id || "-" }}</div><div class="kpi-sub">training ledger current run</div></article>
              <article class="card kpi"><div class="kpi-label">Runs</div><div class="kpi-value">{{ microTrainingSummary.run_count || 0 }}</div><div class="kpi-sub">normalized run samples</div></article>
              <article class="card kpi"><div class="kpi-label">Symbol Samples</div><div class="kpi-value">{{ microTrainingSummary.symbol_sample_count || microTrainingSummary.symbol_count || 0 }}</div><div class="kpi-sub">run / symbol evidence rows</div></article>
              <article class="card kpi"><div class="kpi-label">Coverage</div><div class="kpi-value flat">{{ formatPct(microTrainingCoverageRatio) }}</div><div class="kpi-sub">audit run coverage</div></article>
              <article class="card kpi"><div class="kpi-label">CVD / OFI</div><div class="kpi-value flat">{{ microCoverageMetric("cvd") }} / {{ microCoverageMetric("ofi") }}</div><div class="kpi-sub">training metric SLA</div></article>
              <article class="card kpi"><div class="kpi-label">Spread / Depth</div><div class="kpi-value warn">{{ microCoverageMetric("spread_bps") }} / {{ microCoverageMetric("depth_imbalance") }}</div><div class="kpi-sub">book-cost coverage / depth imbalance</div></article>
              <article class="card kpi"><div class="kpi-label">Data Plane Ready</div><div class="kpi-value flat">{{ microTrainingMetricCoverage.data_plane_ready_count || 0 }}</div><div class="kpi-sub">symbols with readable CVD / OFI / book plane</div></article>
              <article class="card kpi"><div class="kpi-label">Training Usable</div><div class="kpi-value flat">{{ microTrainingMetricCoverage.training_usable_count || 0 }}</div><div class="kpi-sub">AI-ready technical samples</div></article>
              <article class="card kpi"><div class="kpi-label">Reliability</div><div class="kpi-value flat">{{ microTrainingMetricCoverage.avg_technical_reliability_score === null || microTrainingMetricCoverage.avg_technical_reliability_score === undefined ? "-" : Number(microTrainingMetricCoverage.avg_technical_reliability_score).toFixed(2) }}</div><div class="kpi-sub">diagnostic score only</div></article>
              <article class="card kpi"><div class="kpi-label">Degraded</div><div class="kpi-value flat">{{ microTrainingMetricCoverage.training_degraded_count || 0 }}</div><div class="kpi-sub">blocked samples with missing reason</div></article>
            </div>
          </div>
        </div>
        <div class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Current Run Micro Samples</div><span class="tag">{{ microTrainingRunRows.length }} lines</span></div>
          <div class="panel-body table-wrap">
            <table class="mini-table">
              <thead><tr><th>line</th><th>mode</th><th>status</th><th>generated</th><th>source</th><th>reason</th></tr></thead>
              <tbody>
                <tr v-for="row in microTrainingRunRows" :key="`${row.run_id}-${row.strategy_line}-${row.micro_mode}`">
                  <td>{{ lineNames[row.strategy_line] || row.strategy_line }}</td>
                  <td>{{ row.micro_mode }}</td>
                  <td><span class="tag" :class="statusClass(row.status)">{{ row.status }}</span></td>
                  <td>{{ row.generated_at || "-" }}</td>
                  <td>{{ row.source_confidence || "-" }}</td>
                  <td>{{ (row.reason_codes || []).slice(0, 3).join(", ") || row.missing_reason || "-" }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Symbol Training Drilldown</div><span class="tag blue">{{ microTrainingSymbolRows.length }} rows</span></div>
          <div class="panel-body table-wrap">
            <table class="mini-table">
              <thead><tr><th>symbol</th><th>line</th><th>mode</th><th>state</th><th>plane</th><th>align</th><th>z</th><th>score</th><th>CVD</th><th>OFI</th><th>spread bps</th><th>depth</th><th>depth src</th><th>book age</th><th>exec</th><th>TQ</th><th>net R</th><th>technical reason</th><th>reasons</th></tr></thead>
              <tbody>
                <tr v-for="row in microTrainingSymbolRows" :key="row.sample_id">
                  <td>{{ row.symbol }}</td>
                  <td>{{ lineNames[row.strategy_line] || row.strategy_line }}</td>
                  <td>{{ row.micro_mode }}</td>
                  <td><span class="tag" :class="row.accepted ? 'good' : row.blocked ? 'bad' : 'blue'">{{ row.ready_state || row.confirmation_state || "-" }}</span></td>
                  <td><span class="tag" :class="row.micro_data_plane_ready ? 'good' : 'bad'">{{ row.micro_data_plane_ready === null || row.micro_data_plane_ready === undefined ? "-" : row.micro_data_plane_ready ? "ready" : "blocked" }}</span></td>
                  <td><span class="tag" :class="statusClass(row.alignment_state)">{{ row.alignment_state || "-" }}</span></td>
                  <td><span class="tag" :class="row.z_state === 'z_ready' ? 'good' : 'warn'">{{ row.z_state || "-" }}</span></td>
                  <td>{{ row.technical_reliability_score === null || row.technical_reliability_score === undefined ? "-" : Number(row.technical_reliability_score).toFixed(2) }}</td>
                  <td>{{ microTrainingValue(row, "cvd") }}</td>
                  <td>{{ microTrainingValue(row, "ofi") }}</td>
                  <td>{{ row.spread_bps === null || row.spread_bps === undefined ? microTrainingValue(row, "spread") : microTrainingValue(row, "spread_bps") }}</td>
                  <td>{{ microTrainingValue(row, "depth_imbalance") }}</td>
                  <td>{{ row.depth_source || "-" }}</td>
                  <td>{{ row.top_book_age_ms === null || row.top_book_age_ms === undefined ? microTrainingValue(row, "bookticker_age_ms") : microTrainingValue(row, "top_book_age_ms") }}</td>
                  <td>{{ row.executable === null || row.executable === undefined ? "-" : row.executable }}</td>
                  <td>{{ row.trade_quality_root_cause || row.paper_status || row.trade_plan_status || "-" }}</td>
                  <td :class="pnlClass(row.net_R)">{{ signedR(row.net_R) }}</td>
                  <td class="path">{{ row.not_training_usable_reason || row.readiness_block_reason || row.z_missing_reason || row.depth_missing_reason || row.missing_reason || "-" }}</td>
                  <td>{{ (row.reason_codes || []).slice(0, 3).join(", ") || "-" }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
        <div class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Micro Training Run History</div><span class="tag blue">{{ microTrainingHistoryRows.length }} runs</span></div>
          <div class="panel-body table-wrap">
            <table class="mini-table">
              <thead><tr><th>run</th><th>cycle</th><th>generated</th><th>lines</th><th>ready</th><th>technical blocked</th></tr></thead>
              <tbody>
                <tr v-for="row in microTrainingHistoryRows" :key="row.run_id">
                  <td>{{ row.run_id }}</td><td>{{ row.cycle_id || "-" }}</td><td>{{ row.generated_at || "-" }}</td>
                  <td>{{ row.line_count || 0 }}</td><td>{{ row.ready_lines || 0 }}</td><td>{{ row.technical_blocked_lines || 0 }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section v-else-if="activePage === 'pipeline'">
        <div class="pipeline-page">
          <div class="pipeline-header">
            <div>
              <h2>Runtime Trading Dashboard</h2>
              <p>系统当前处于三层确认模式：without / fast / full 并行对照，重点监控 pipeline 进度、trade plan 可执行性与策略运行状态。</p>
            </div>
            <div class="pipeline-actions">
              <span class="pill ok">API healthy</span>
              <span class="pill info">{{ pipelineUi.mode }}</span>
              <div class="strategy-select" :class="{ disabled: isPipelineRunning || loading }">
                <button
                  v-for="meta in pipelineStrategyMeta"
                  :key="meta.line"
                  class="strategy-toggle"
                  :class="{ active: selectedPipelineLines.includes(meta.line) }"
                  :disabled="!pipelineTrulyIdle || loading"
                  @click="togglePipelineLine(meta.line)"
                >
                  {{ lineNames[meta.line] }}
                </button>
              </div>
              <button class="btn primary" :disabled="!canRunOnce" :title="runOnceDisabledReason" @click="runAllOnce"><Play />Run Once</button>
              <button
                class="btn"
                :class="isPipelineRunning ? 'stop' : 'cycle'"
                :disabled="cycleToggleDisabled"
                :title="runCycleDisabledReason"
                @click="toggleFullChainCycle"
              >
                <Square v-if="isPipelineRunning" />
                <RefreshCw v-else />
                {{ isPipelineRunning ? 'Stop Cycle' : 'Run Cycle' }}
              </button>
              <button class="btn" @click="startRuntime"><Play />Start Runtime</button>
              <button class="btn" @click="restartRuntime"><RefreshCw />Restart Runtime</button>
              <button class="btn stop" :disabled="!canStopPipeline" @click="api.pipelineStop().then(refreshAll).then(() => pipelineUi.mode = 'idle')"><Square />Stop</button>
            </div>
          </div>

          <section class="grid-kpi">
            <article class="card kpi">
              <div class="kpi-label">API Status</div>
              <div class="kpi-value up">ok</div>
              <div class="kpi-sub">FastAPI facade · websocket proxy ready</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Micro Daemon</div>
              <div class="kpi-value status-value"><span v-if="isRunningHealth(microHealth)" class="pulse-dot inline"></span>{{ microHealth?.status || "unknown" }}</div>
              <div class="kpi-sub">{{ healthText(microHealth) }}</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Paper Daemon</div>
              <div class="kpi-value status-value"><span v-if="isRunningHealth(paperHealth)" class="pulse-dot inline"></span>{{ paperHealth?.status || "unknown" }}</div>
              <div class="kpi-sub">{{ healthText(paperHealth) }}</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Strategy4 / 异动肆号</div>
              <div class="kpi-value status-value">
                <span v-if="['ok','running'].includes(String(strategy4RuntimeState).toLowerCase())" class="pulse-dot inline"></span>
                {{ strategy4RuntimeState }}
              </div>
              <div class="kpi-sub">observe daemon · pool {{ strategy4PoolCount }} · still_wait {{ strategy4PoolCounts.still_wait || 0 }}</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Pipeline Runtime</div>
              <div class="kpi-value status-value"><span v-if="['once','cycle'].includes(pipelineUi.mode)" class="pulse-dot inline"></span>{{ pipelineUi.mode === 'cycle_waiting' ? 'waiting' : pipelineUi.mode === 'idle' ? 'idle' : pipelineUi.mode === 'done' ? 'completed' : 'running' }}</div>
              <div class="kpi-sub">{{ isCycleWaiting ? `next cycle in ${pipeline?.next_cycle_eta_sec ?? pipelineWatchdog?.next_cycle_eta_sec ?? '-'}s` : (pipeline?.progress?.current_stage || (pipeline?.lock_stale ? 'stale lock detected' : pipeline?.job_running ? 'job running' : 'waiting for command')) }}</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Run Cycle Watchdog</div>
              <div class="kpi-value status-value">
                <span v-if="pipelineWatchdog?.health === 'ok'" class="pulse-dot inline"></span>
                {{ pipelineWatchdog?.health || 'unknown' }}
              </div>
              <div class="kpi-sub">{{ watchdogStateText }} · next {{ pipelineWatchdog?.next_cycle_eta_sec ?? '-' }}s</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Cycle</div>
              <div class="kpi-value flat">{{ pipelineUi.cycleCount }}</div>
              <div class="kpi-sub">run once 完成后停止；run cycle 自动进入下一轮</div>
            </article>
            <article class="card kpi">
              <div class="kpi-label">Job PID</div>
              <div class="kpi-value">{{ pipeline?.active_job?.pid || '-' }}</div>
              <div class="kpi-sub">{{ pipeline?.progress?.run_id || 'pending run id' }}</div>
            </article>
          </section>

          <section class="pipeline-layout">
            <article class="card">
              <div class="panel-header">
                <div class="panel-title"><span class="accent"></span>Strategy Pipeline Progress</div>
                <span class="pill info">overall {{ pipelineUi.overall }}%</span>
              </div>
              <div class="panel-body strategy-stack">
                <div
                  v-for="meta in pipelineStrategyMeta"
                  :key="meta.line"
                  class="strategy-card"
                  :class="[{ running: ['once','cycle','cycle_waiting'].includes(pipelineUi.mode) }, `line-state-${pipelineLineState(meta.line)}`]"
                >
                  <div class="strategy-top">
                    <div>
                      <div class="strategy-name">{{ meta.title }}</div>
                      <div class="strategy-desc">{{ meta.desc }}</div>
                    </div>
                    <div class="strategy-num">
                      <span class="tag" :class="pipelineLineStateClass(meta.line)">{{ pipelineLineStateLabel(meta.line) }}</span>
                      <div class="strategy-percent">{{ Math.round(pipelineUi.lines[meta.line]?.percent || 0) }}%</div>
                      <div class="strategy-stage">{{ pipelineUi.lines[meta.line]?.stage || 'waiting' }}</div>
                      <div class="strategy-stage">{{ pipelineLineReason(meta.line) }}</div>
                      <div class="strategy-stage">
                        <span class="tag" :class="pipelineUi.lines[meta.line]?.output_fresh ? 'good' : 'warn'">
                          {{ pipelineUi.lines[meta.line]?.output_fresh ? 'fresh' : 'not fresh' }}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div class="progress-track">
                    <div
                      class="progress-fill"
                      :class="meta.className"
                      :style="{ width: `${Math.round(pipelineUi.lines[meta.line]?.percent || 0)}%` }"
                    ></div>
                  </div>
                  <div class="stage-row">
                    <div
                      v-for="(stage, idx) in pipelineFunnelStages(meta.line)"
                      :key="stage.code"
                      class="stage"
                      :class="[stageState(meta.line, idx), funnelStageClass(stage)]"
                      :title="funnelStageTitle(stage)"
                    >
                      <b>{{ stage.code }}</b>
                      <span>{{ stage.name }}</span>
                      <small>{{ funnelStageSubtitle(stage) }}</small>
                    </div>
                  </div>
                  <div class="strategy-meta-row">
                    <span>run {{ pipelineUi.lines[meta.line]?.run_id || '-' }}</span>
                    <span>output {{ pipelineUi.lines[meta.line]?.output_generated_at || '-' }}</span>
                  </div>
                  <div class="strategy-meta-row funnel-breakpoint-row">
                    <span>funnel {{ funnelBreakpointLabel(meta.line) }}</span>
                    <span>{{ pipelineFunnelLine(meta.line)?.generated_at || '-' }}</span>
                  </div>
                </div>
              </div>
            </article>

            <aside class="side-stack">
              <article class="card">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Run Mode</div><span class="pill warn">{{ pipelineUi.mode === 'cycle' ? 'loop' : pipelineUi.mode === 'cycle_waiting' ? 'waiting next cycle' : pipelineUi.mode }}</span></div>
                <div class="panel-body mode-list">
                  <div class="mode-item"><strong>Run Once <span class="tag">once</span></strong><p>三个策略进度条跑到 100% 后，当前轮次结束并停止。</p></div>
                  <div class="mode-item"><strong>Run Cycle <span class="tag good">loop</span></strong><p>三个策略全部完成后自动清空进度，继续下一轮循环执行。</p></div>
                </div>
              </article>
              <article class="card">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Runtime Stats</div><span class="pill ok">live</span></div>
                <div class="panel-body stats-grid">
                  <div class="stat"><span>Overall Progress</span><strong>{{ pipelineUi.overall }}%</strong></div>
                  <div class="stat"><span>Job Running</span><strong>{{ pipeline?.job_running ? 'yes' : 'no' }}</strong></div>
                  <div class="stat"><span>Lock Stale</span><strong>{{ pipeline?.lock_stale ? 'yes' : 'no' }}</strong></div>
                  <div class="stat"><span>Lock Reconcile</span><strong>{{ pipeline?.registry_health?.reconcile_action || 'none' }}</strong></div>
                  <div class="stat wide"><span>Lock Reasons</span><strong>{{ (pipeline?.registry_health?.reconcile_reason_codes || []).join(', ') || '-' }}</strong></div>
                  <div class="stat"><span>Strategy4 Pool</span><strong>{{ strategy4PoolCount }}</strong></div>
                  <div class="stat"><span>Strategy4 Due</span><strong>{{ strategy4Status.due_count ?? '-' }}</strong></div>
                  <div class="stat wide"><span>Strategy4 Mode</span><strong>observe daemon · source without_micro · interval 5m</strong></div>
                </div>
              </article>
              <article class="card">
                <div class="panel-header">
                  <div class="panel-title"><span class="accent"></span>Run Cycle Watchdog</div>
                  <span class="tag" :class="watchdogHealthClass">{{ pipelineWatchdog?.health || 'unknown' }}</span>
                </div>
                <div class="panel-body mode-list">
                  <div class="mode-item">
                    <strong>State <span class="tag blue">{{ watchdogStateText }}</span></strong>
                    <p>display run {{ pipelineWatchdog?.display_run_id || '-' }} · latest {{ pipelineWatchdog?.latest_report?.run_id || '-' }}</p>
                  </div>
                  <div class="mode-item">
                    <strong>Heartbeat <span class="tag" :class="pipelineWatchdog?.reason_codes?.length ? 'warn' : 'good'">{{ pipelineWatchdog?.reason_codes?.length || 0 }} reasons</span></strong>
                    <p>lock {{ pipelineWatchdog?.watchdog?.lock_age_sec ?? '-' }}s · progress {{ pipelineWatchdog?.watchdog?.progress_age_sec ?? '-' }}s · next {{ pipelineWatchdog?.next_cycle_eta_sec ?? '-' }}s</p>
                  </div>
                  <div class="mode-item">
                    <strong>Daemons <span class="tag good">live</span></strong>
                    <p>micro {{ pipelineWatchdog?.micro_daemon?.heartbeat_age_sec ?? '-' }}s · paper {{ pipelineWatchdog?.paper_daemon?.heartbeat_age_sec ?? '-' }}s</p>
                  </div>
                </div>
              </article>
              <article class="card">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Runtime Logs</div><span class="pill warn">tail</span></div>
                <div class="panel-body">
                  <div class="log-box">
                    <div v-for="(line, idx) in pipelineUi.logs" :key="idx" class="log-line" :class="line.type === 'warn' ? 'log-warn' : 'log-ok'">
                      {{ typeof line === 'string' ? line : line.text }}
                    </div>
                  </div>
                </div>
              </article>
            </aside>
          </section>

          <section class="footer-grid">
            <article class="card panel">
              <div class="panel-header"><div class="panel-title"><span class="accent"></span>Freshness Monitor</div><span class="tag good">stream aligned</span></div>
              <div class="panel-body">
                <table>
                  <thead><tr><th>Source</th><th>Age</th><th>Status</th><th>Path</th></tr></thead>
                  <tbody>
                    <tr v-for="row in freshnessRows" :key="row.source">
                      <td>{{ row.source }}</td>
                      <td>{{ row.age }}</td>
                      <td><span class="tag" :class="statusClass(row.status)">{{ row.status }}</span></td>
                      <td class="path">{{ row.path }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </article>
            <article class="card panel">
              <div class="panel-header"><div class="panel-title"><span class="accent"></span>Latest Audit</div><span class="tag warn">warning</span></div>
              <div class="panel-body">
                <div class="audit-row"><div class="audit-code">P1/P2</div><div class="audit-name">Universe and snapshot contract</div><span class="tag good">pass</span></div>
                <div class="audit-row"><div class="audit-code">P3</div><div class="audit-name">Micro signal usability gate</div><span class="tag good">pass</span></div>
                <div class="audit-row"><div class="audit-code">P10</div><div class="audit-name">ABC line isolation</div><span class="tag warn">warn</span></div>
              </div>
            </article>
          </section>
        </div>
      </section>

      <section v-else-if="activePage === 'snapshot'">
        <PageHeader title="Snapshot" subtitle="Step1 / Step1.5 daemon source, freshness, REST cooldown and downstream gate evidence." />
        <div class="grid-kpi">
          <article class="card kpi">
            <div class="kpi-label">Daemon</div>
            <div class="kpi-value status-value"><span v-if="isRunningHealth(snapshotDaemonSummary)" class="pulse-dot inline"></span>{{ snapshotDaemonSummary?.daemon_status || snapshotDaemonSummary?.status || "unknown" }}</div>
            <div class="kpi-sub">{{ healthText(snapshotDaemonSummary) }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-label">Heartbeat Age</div>
            <div class="kpi-value flat">{{ secondsText(snapshotDaemonSummary?.heartbeat_age_sec) }}</div>
            <div class="kpi-sub">stale after {{ secondsText(snapshotDaemonSummary?.stale_after_sec) }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-label">Fresh / Usable / Blocked</div>
            <div class="kpi-value">{{ snapshotFreshnessCounts?.fresh || 0 }} / {{ snapshotFreshnessCounts?.stale_usable || 0 }} / {{ snapshotFreshnessCounts?.stale_blocked || 0 }}</div>
            <div class="kpi-sub">{{ snapshotWarmup?.ready_status_detail || snapshotWarmup?.status || "warmup unknown" }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-label">REST Circuit</div>
            <div class="kpi-value status-value">{{ runtimeRestBudget?.rest_circuit_state || snapshotDaemonSummary?.rest_circuit_state || snapshotLatest?.rest_circuit_state || "unknown" }}</div>
            <div class="kpi-sub">{{ runtimeRestBudget?.rest_recovery_stage || snapshotWarmup?.freshness_degradation_reason || "no reason" }}</div>
          </article>
        </div>
        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Daemon Watchdog</div><span class="tag" :class="statusClass(snapshotDaemonSummary?.watchdog_status)">{{ snapshotDaemonSummary?.watchdog_status || "unknown" }}</span></div>
            <div class="panel-body">
              <div class="notice-list">
                <div class="notice"><span>current shard</span><span class="tag blue">{{ snapshotDaemonSummary?.current_shard_id || snapshotLatest?.current_shard_id || "-" }}</span></div>
                <div class="notice"><span>shard size</span><span class="tag blue">{{ runtimeRestBudget?.current_shard_size ?? snapshotDaemonSummary?.current_shard_size ?? "-" }} -> {{ runtimeRestBudget?.next_shard_size ?? snapshotDaemonSummary?.next_shard_size ?? "-" }}</span></div>
                <div class="notice"><span>recovery streak</span><span class="tag">{{ runtimeRestBudget?.rest_consecutive_successful_shards ?? snapshotDaemonSummary?.rest_consecutive_successful_shards ?? "-" }} / {{ runtimeRestBudget?.rest_success_required_for_close ?? snapshotDaemonSummary?.rest_success_required_for_close ?? "-" }}</span></div>
                <div class="notice"><span>next cursor</span><span class="tag blue">{{ snapshotDaemonSummary?.next_shard_cursor ?? snapshotLatest?.next_shard_cursor ?? "-" }}</span></div>
                <div class="notice"><span>queue depth</span><span class="tag">{{ snapshotDaemonSummary?.queue_depth ?? "-" }}</span></div>
                <div class="notice"><span>last success</span><span class="tag good">{{ snapshotDaemonSummary?.last_successful_shard_at || "-" }}</span></div>
                <div class="notice" v-if="snapshotDaemonSummary?.last_error"><span>{{ snapshotDaemonSummary.last_error }}</span><span class="tag bad">error</span></div>
              </div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Source Mix</div><span class="tag blue">{{ snapshotLatest?.market_snapshot_source || "snapshot" }}</span></div>
            <div class="panel-body">
              <div class="plan-grid">
                <div v-for="(value, key) in snapshotSourceMix" :key="key" class="metric"><span>{{ key }}</span><strong>{{ value }}</strong></div>
              </div>
              <div class="notice-list compact">
                <div class="notice"><span>REST requests</span><span class="tag">{{ runtimeRestBudget?.rest_request_count ?? snapshotDaemonSummary?.rest_request_count ?? "-" }}</span></div>
                <div class="notice"><span>status codes</span><span class="tag blue">{{ JSON.stringify(runtimeRestBudget?.rest_status_code_counts || snapshotDaemonSummary?.rest_status_code_counts || {}) }}</span></div>
                <div class="notice"><span>cooldown until</span><span class="tag warn">{{ runtimeRestBudget?.cooldown_until || snapshotDaemonSummary?.cooldown_until || snapshotDaemonSummary?.rest_cooldown_until || "-" }}</span></div>
              </div>
              <div class="json-box small">{{ JSON.stringify(snapshotLatest?.reason_codes || snapshotDaemonSummary?.reason_codes || [], null, 2) }}</div>
            </div>
          </article>
        </div>
        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Symbol Freshness Drilldown</div><span class="tag blue">{{ snapshotRows.length }} rows</span></div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Symbol</th><th>Pool</th><th>Source</th><th>Age</th><th>Freshness</th><th>Downstream</th><th>Shard</th><th>Reasons</th></tr></thead>
              <tbody>
                <tr v-for="row in snapshotRows.slice(0, 300)" :key="row.symbol">
                  <td>{{ row.symbol }}</td>
                  <td>{{ row.primary_pool || row.universe_profile?.business_pool || "-" }}</td>
                  <td>{{ row.item_snapshot_source || row.snapshot_source_priority || "-" }}</td>
                  <td>{{ secondsText(row.item_snapshot_age_sec) }}</td>
                  <td><span class="tag" :class="statusClass(row.item_freshness_status)">{{ row.item_freshness_status || "unknown" }}</span></td>
                  <td><span class="tag" :class="row.item_downstream_allowed === false ? 'bad' : 'good'">{{ row.item_downstream_allowed === false ? "blocked" : "allowed" }}</span></td>
                  <td>{{ row.shard_id || "-" }}</td>
                  <td class="reason-cell">{{ (row.reason_codes || []).slice(0, 5).join(", ") || "-" }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>
        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Raw Snapshot Daemon Payload</div><span class="tag warn">debug</span></div>
          <pre class="json-box">{{ JSON.stringify(snapshotDebugPayload, null, 2) }}</pre>
        </article>
      </section>

      <section v-else-if="activePage === 'plans'">
        <PageHeader title="Trade Plans" subtitle="Multi-strategy trade plan funnel: candidates, blockers, executable plans, and paper handoff." />

        <div class="audit-kpi-grid">
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Run ID</span><span class="status-icon"><ListChecks /></span></div>
            <div class="kpi-value">{{ tradePlanRunLabel }}</div>
            <div class="kpi-sub">cycle {{ tradePlanFunnel.cycle_id || "-" }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Plans</span><span class="status-icon"><ShieldCheck /></span></div>
            <div class="kpi-value">{{ tradePlanFunnelCounts.total_plans || 0 }} / {{ tradePlanFunnelCounts.executable || 0 }}</div>
            <div class="kpi-sub">total / executable</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Paper</span><span class="status-icon"><Bell /></span></div>
            <div class="kpi-value">{{ tradePlanFunnelCounts.paper_orders || 0 }} / {{ tradePlanFunnelCounts.paper_skips || 0 }}</div>
            <div class="kpi-sub">orders / skips</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Handoff</span><span class="status-icon"><FileWarning /></span></div>
            <div class="kpi-value" :class="(tradePlanFunnelCounts.paper_missing || 0) ? 'bad' : 'good'">{{ tradePlanFunnelCounts.paper_missing || 0 }}</div>
            <div class="kpi-sub">executable not seen by paper</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">异动肆号</span><span class="status-icon"><ListChecks /></span></div>
            <div class="kpi-value">{{ strategy4PlanSummary.count }} / {{ strategy4PlanSummary.executable }}</div>
            <div class="kpi-sub">
              latest_trade_plan_strategy4 · {{ strategy4PlanSummary.stale ? strategy4PlanSummary.stale_reason || "stale" : "fresh" }} · micro none
            </div>
          </article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Strategy Lines</div>
            <span class="tag blue">{{ tradePlanFunnelLines.length }} lines</span>
          </div>
          <div class="panel-body">
            <div class="trade-line-tabs">
              <button
                v-for="line in tradePlanFunnelLines"
                :key="line.line"
                class="trade-line-tab"
                :class="{ active: selectedTradePlanLineDoc?.line === line.line, skipped: line.skipped }"
                @click="selectedTradePlanLine = line.line"
              >
                <span class="trade-line-name">{{ line.display_name || line.line }}</span>
                <span class="trade-line-meta">{{ line.line }} ? {{ line.generated_at || "-" }}</span>
                <span class="tag-row">
                  <em class="tag" :class="line.skipped ? 'warn' : 'blue'">{{ line.skipped ? "skipped" : "selected" }}</em>
                  <em class="tag" :class="line.counts?.executable ? 'good' : 'warn'">exec {{ line.counts?.executable || 0 }}</em>
                  <em class="tag" :class="(line.counts?.blocked || 0) ? 'warn' : 'good'">blocked {{ line.counts?.blocked || 0 }}</em>
                </span>
              </button>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>异动肆号 / WAIT 复查池</div>
            <span class="tag" :class="['ok','running'].includes(String(strategy4RuntimeState).toLowerCase()) ? 'good' : 'warn'">
              {{ strategy4RuntimeState }}
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid compact-grid">
              <div class="metric"><span>pool</span><strong>{{ strategy4PoolCount }}</strong></div>
              <div class="metric"><span>observing</span><strong>{{ strategy4PoolCounts.observing || 0 }}</strong></div>
              <div class="metric"><span>still wait</span><strong>{{ strategy4PoolCounts.still_wait || 0 }}</strong></div>
              <div class="metric"><span>executable</span><strong>{{ strategy4PoolCounts.executable || 0 }}</strong></div>
              <div class="metric"><span>hard deny</span><strong>{{ strategy4PoolCounts.hard_denied || 0 }}</strong></div>
              <div class="metric"><span>attempts</span><strong>{{ strategy4Attempts.count || 0 }}</strong></div>
            </div>
            <div class="table-scroll">
              <table>
                <thead><tr><th>Symbol</th><th>Status</th><th>Side</th><th>Attempts</th><th>Next Check</th><th>Last Action</th><th>Reasons</th></tr></thead>
                <tbody>
                  <tr v-for="row in strategy4PoolRows.slice(0, 30)" :key="row.symbol">
                    <td><strong>{{ row.symbol }}</strong></td>
                    <td><span class="tag" :class="row.status === 'executable' ? 'good' : row.status === 'hard_denied' ? 'bad' : 'blue'">{{ row.status || "-" }}</span></td>
                    <td>{{ row.original_side || "-" }} -> {{ row.current_side || "-" }} <span v-if="row.side_changed" class="tag warn">changed</span></td>
                    <td>{{ row.attempt_count || 0 }}</td>
                    <td>{{ row.next_check_at || "-" }}</td>
                    <td>{{ row.last_action || row.last_decision || "-" }} / {{ row.last_entry_mode || "-" }}</td>
                    <td class="reason-cell">{{ (row.last_reason_codes || row.source_reason_codes || []).join(", ") || "-" }}</td>
                  </tr>
                  <tr v-if="!strategy4PoolRows.length"><td colspan="7">No Strategy4 observe rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <div v-if="selectedTradePlanLineDoc" class="wide-grid">
          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>{{ selectedTradePlanLineDoc.display_name }} Funnel</div>
              <span class="tag" :class="selectedTradePlanLineDoc.stale ? 'warn' : 'good'">{{ selectedTradePlanLineDoc.stale ? "stale" : "fresh" }}</span>
            </div>
            <div class="panel-body">
              <div class="funnel-bar">
                <div v-for="stage in tradePlanFunnelStages" :key="stage.key" class="funnel-step">
                  <span>{{ stage.label }}</span>
                  <strong>{{ stage.count }}</strong>
                </div>
              </div>
              <div class="plan-grid compact-grid">
                <div class="metric"><span>generated</span><strong>{{ tradePlanSelectedLineCounts.total_plans || 0 }}</strong></div>
                <div class="metric"><span>market</span><strong>{{ tradePlanSelectedLineCounts.market || 0 }}</strong></div>
                <div class="metric"><span>wait</span><strong>{{ tradePlanSelectedLineCounts.wait || 0 }}</strong></div>
                <div class="metric"><span>blocked</span><strong>{{ tradePlanSelectedLineCounts.blocked || 0 }}</strong></div>
                <div class="metric"><span>paper orders</span><strong>{{ tradePlanSelectedLineCounts.paper_orders || 0 }}</strong></div>
                <div class="metric"><span>paper missing</span><strong>{{ tradePlanSelectedLineCounts.paper_missing || 0 }}</strong></div>
              </div>
              <div v-if="selectedTradePlanLineDoc.stale" class="notice warn">
                <span>{{ selectedTradePlanLineDoc.stale_reason || "trade plan output is stale" }}</span>
                <span class="tag warn">{{ selectedTradePlanLineDoc.output_run_id || "-" }}</span>
              </div>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Top Blockers</div>
              <span class="tag blue">{{ tradePlanReasonGroups.length }} groups</span>
            </div>
            <div class="panel-body table-scroll reason-table">
              <table>
                <thead><tr><th>Reason</th><th>Category</th><th>Count</th><th>Sample</th></tr></thead>
                <tbody>
                  <tr v-for="group in tradePlanReasonGroups.slice(0, 24)" :key="group.reason">
                    <td class="reason-cell">{{ group.reason }}</td>
                    <td><span class="tag blue">{{ group.category }}</span></td>
                    <td>{{ group.count }}</td>
                    <td class="reason-cell">{{ (group.symbols || []).slice(0, 6).join(", ") || "-" }}</td>
                  </tr>
                  <tr v-if="!tradePlanReasonGroups.length"><td colspan="4">No blockers for this line.</td></tr>
                </tbody>
              </table>
            </div>
          </article>
        </div>

        <article v-if="selectedTradePlanLineDoc" class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Symbol Drilldown</div>
            <span class="tag blue">{{ tradePlanSymbolRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll trade-plan-symbol-table">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th><th>Side</th><th>Action</th><th>Entry</th><th>SL</th><th>TP</th><th>RR</th><th>TP Policy</th><th>Risk</th><th>Paper</th><th>Reasons</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in tradePlanSymbolRows" :key="`${row.symbol}-${row.lineage?.source_plan_hash || row.action}`">
                  <td><strong>{{ row.symbol }}</strong><div class="path">{{ row.lineage?.source_plan_hash || "-" }}</div></td>
                  <td><span class="tag" :class="row.side === 'SHORT' ? 'warn' : 'good'">{{ row.side || "-" }}</span></td>
                  <td><span class="tag" :class="row.executable ? 'good' : 'warn'">{{ row.action || "-" }} / {{ row.entry_mode || "-" }}</span></td>
                  <td>{{ row.risk?.entry ?? "-" }}</td>
                  <td>{{ row.risk?.stop_loss ?? "-" }}</td>
                  <td>{{ row.risk?.take_profit ?? "-" }}</td>
                  <td>{{ row.risk?.rr ?? "-" }}</td>
                  <td>
                    <span class="tag" :class="row.guards?.tp_was_capped ? 'warn' : 'blue'">
                      {{ row.guards?.tp_target_policy_mode || "structure" }}
                    </span>
                    <div class="path">
                      {{ row.guards?.final_reward_bps ?? "-" }} bps · {{ row.guards?.final_gross_rr ?? "-" }}R gross
                    </div>
                    <div class="path">
                      {{ row.guards?.tp_target_policy_basis || "gross" }} · net {{ row.guards?.final_net_rr ?? "-" }}R
                      <span v-if="row.guards?.configured_target_net_rr">/ target {{ row.guards?.configured_target_net_rr }}R</span>
                    </div>
                    <div v-if="row.guards?.tp_cap_reason || row.guards?.tp_reject_reason" class="path">
                      {{ row.guards?.tp_reject_reason || row.guards?.tp_cap_reason }}
                    </div>
                  </td>
                  <td>{{ row.risk?.planned_loss_usdt ?? "-" }}</td>
                  <td>
                    <span class="tag" :class="row.paper?.paper_status === 'order' ? 'good' : row.paper?.paper_status === 'missing' ? 'bad' : 'warn'">
                      {{ row.paper?.paper_status || "-" }}
                    </span>
                    <div class="path">{{ row.paper?.paper_order_id || row.paper?.skip_reason || "-" }}</div>
                  </td>
                  <td class="reason-cell">{{ (row.reason_codes || []).join(", ") || "-" }}</td>
                </tr>
                <tr v-if="!tradePlanSymbolRows.length"><td colspan="11">No plan rows for this strategy line.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <details class="card panel debug-details">
          <summary>Raw Trade Plan Debug Payload</summary>
          <pre class="json-box">{{ JSON.stringify({ funnel: tradePlanFunnel, raw: tradePlans }, null, 2) }}</pre>
        </details>
      </section>

      <section v-else-if="activePage === 'strategy4'">
        <PageHeader title="Strategy4 / 异动肆号" subtitle="策略1 without_micro 的 WAIT 复查 sidecar：常驻观察、5分钟复查、完整重判方向，只有 executable 才进入下游消费边界。" />

        <div class="audit-kpi-grid">
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Daemon State</span><span class="status-icon"><Activity /></span></div>
            <div class="kpi-value" :class="statusClass(strategy4RuntimeState)">{{ strategy4RuntimeState }}</div>
            <div class="kpi-sub">pid {{ strategy4Status.pid || "-" }} · observe daemon</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Observe Pool</span><span class="status-icon"><ListChecks /></span></div>
            <div class="kpi-value">{{ strategy4PoolCount }}</div>
            <div class="kpi-sub">still_wait {{ strategy4PoolCounts.still_wait || 0 }} · executable {{ strategy4PoolCounts.executable || 0 }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Due / Attempts</span><span class="status-icon"><Clock3 /></span></div>
            <div class="kpi-value">{{ strategy4Status.due_count ?? 0 }} / {{ strategy4Attempts.count || strategy4AttemptRows.length }}</div>
            <div class="kpi-sub">next scheduler checks · attempt ledger</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Trade Plan</span><span class="status-icon"><CandlestickChart /></span></div>
            <div class="kpi-value">{{ strategy4PlanSummary.count }} / {{ strategy4PlanSummary.executable }}</div>
            <div class="kpi-sub">{{ strategy4PlanSummary.output_run_id }} · {{ strategy4PlanSummary.stale ? strategy4PlanSummary.stale_reason || "stale" : "fresh" }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Last Generated</span><span class="status-icon"><RefreshCw /></span></div>
            <div class="kpi-value flat">{{ ageLabel(strategy4Status.last_generated_at || strategy4Pool.generated_at) }}</div>
            <div class="kpi-sub">{{ strategy4Status.last_generated_at || strategy4Pool.generated_at || "-" }}</div>
          </article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Observe Pool</div>
            <span class="tag blue">{{ strategy4PoolRows.length }} symbols</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Symbol</th><th>Status</th><th>Original Side</th><th>Current Side</th><th>Changed</th><th>Attempts</th><th>Next Check</th><th>Last Action</th><th>Source</th><th>Reasons</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in strategy4PoolRows" :key="row.symbol">
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td><span class="tag" :class="row.status === 'executable' ? 'good' : row.status === 'hard_denied' ? 'bad' : 'blue'">{{ row.status || "-" }}</span></td>
                  <td>{{ row.original_side || "-" }}</td>
                  <td>{{ row.current_side || "-" }}</td>
                  <td><span class="tag" :class="row.side_changed ? 'warn' : 'good'">{{ row.side_changed ? "yes" : "no" }}</span></td>
                  <td>{{ row.attempt_count || 0 }}</td>
                  <td>{{ row.next_check_at || "-" }}</td>
                  <td>{{ row.last_action || row.last_decision || "-" }} / {{ row.last_entry_mode || "-" }}</td>
                  <td>
                    <div>{{ row.source_run_id || row.lineage?.source_plan_run_id || "-" }}</div>
                    <div class="path">{{ row.lineage?.source_plan_hash || "-" }}</div>
                  </td>
                  <td class="reason-cell">{{ (row.last_reason_codes || row.source_reason_codes || []).join(", ") || "-" }}</td>
                </tr>
                <tr v-if="!strategy4PoolRows.length"><td colspan="10">No Strategy4 observe pool rows from FastAPI.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Attempt Timeline</div>
            <span class="tag blue">{{ strategy4AttemptRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Attempted</th><th>Run</th><th>Cycle</th><th>Symbol</th><th>Side</th><th>Changed</th><th>Action</th><th>Entry Mode</th><th>Executable</th><th>Reasons</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in strategy4AttemptRows.slice(0, 160)" :key="row.attempt_id || `${row.run_id}-${row.symbol}-${row.attempted_at}`">
                  <td>{{ row.attempted_at || "-" }}</td>
                  <td>{{ row.run_id || "-" }}</td>
                  <td>{{ row.cycle_id || "-" }}</td>
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td>{{ row.original_side || "-" }} -> {{ row.current_side || row.decision || "-" }}</td>
                  <td><span class="tag" :class="row.side_changed ? 'warn' : 'good'">{{ row.side_changed ? "yes" : "no" }}</span></td>
                  <td><span class="tag" :class="row.executable ? 'good' : 'blue'">{{ row.action || row.status || "-" }}</span></td>
                  <td>{{ row.entry_mode || "-" }}</td>
                  <td><span class="tag" :class="row.executable ? 'good' : 'warn'">{{ row.executable ? "yes" : "no" }}</span></td>
                  <td class="reason-cell">{{ (row.reason_codes || []).join(", ") || "-" }}</td>
                </tr>
                <tr v-if="!strategy4AttemptRows.length"><td colspan="10">No Strategy4 attempts from FastAPI.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Runtime Contract</div>
            <span class="tag">FastAPI /api/strategy4/*</span>
          </div>
          <div class="panel-body stats-grid">
            <div class="stat"><span>source</span><strong>strategy1 without_micro WAIT</strong></div>
            <div class="stat"><span>interval</span><strong>5m observe scheduler</strong></div>
            <div class="stat"><span>micro</span><strong>none</strong></div>
            <div class="stat"><span>paper boundary</span><strong>executable only</strong></div>
            <div class="stat wide"><span>heartbeat</span><strong>{{ strategy4Heartbeat.updated_at || strategy4Heartbeat.generated_at || "-" }}</strong></div>
            <div class="stat wide"><span>latest plan path</span><strong>{{ strategy4Status.latest_trade_plan_path || "-" }}</strong></div>
          </div>
        </article>

        <details class="card panel debug-details">
          <summary>Raw Strategy4 FastAPI Payload</summary>
          <pre class="json-box">{{ JSON.stringify(strategy4DebugPayload, null, 2) }}</pre>
        </details>
      </section>

      <section v-else-if="activePage === 'strategy5'">
        <PageHeader title="Strategy5 / 异动伍号" subtitle="方向证据支线：跟随 pipeline 运行，复用现有证据，不占 micro slot，输出独立 trade plan 并进入 paper 对比。" />
        <div class="grid-kpi">
          <article class="card kpi"><div class="kpi-label">Runtime</div><div class="kpi-value" :class="statusClass(strategy5Summary.status)">{{ strategy5Summary.status }}</div><div class="kpi-sub">latest plan {{ strategy5Summary.generated_at }}</div></article>
          <article class="card kpi"><div class="kpi-label">Plans</div><div class="kpi-value">{{ strategy5Summary.count }}</div><div class="kpi-sub">strategy5 trade plan rows</div></article>
          <article class="card kpi"><div class="kpi-label">Executable</div><div class="kpi-value up">{{ strategy5Summary.executable }}</div><div class="kpi-sub">paper consumable if market executable</div></article>
          <article class="card kpi"><div class="kpi-label">Evidence</div><div class="kpi-value flat">{{ strategy5Summary.evidence }}</div><div class="kpi-sub">direction evidence vectors</div></article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Direction Evidence Ledger</div>
            <span class="tag blue">{{ strategy5EvidenceRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Symbol</th><th>Trigger</th><th>Legacy</th><th>Label</th><th>Recommendation</th><th>Cont</th><th>Exhaust</th><th>Executable</th><th>Run</th></tr></thead>
              <tbody>
                <tr v-for="row in strategy5EvidenceRows.slice(0, 200)" :key="row.evidence_id || `${row.symbol}-${row.generated_at}`">
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td>{{ row.trigger_side || row.shadow_hypothesis_side || "-" }}</td>
                  <td>{{ row.legacy_side || row.legacy_move_side || "-" }}</td>
                  <td><span class="tag" :class="statusClass(row.label || row.shadow_label)">{{ row.label || row.shadow_label || "-" }}</span></td>
                  <td>{{ row.recommendation || row.shadow_recommendation || "-" }}</td>
                  <td>{{ row.continuation_score ?? "-" }}</td>
                  <td>{{ row.exhaustion_score ?? "-" }}</td>
                  <td><span class="tag" :class="row.executable ? 'good' : 'warn'">{{ row.executable ? "yes" : "no" }}</span></td>
                  <td>{{ row.run_id || "-" }}</td>
                </tr>
                <tr v-if="!strategy5EvidenceRows.length"><td colspan="9" class="empty-state">No Strategy5 evidence yet. Run pipeline with strategy5 selected.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <details class="card panel debug-details">
          <summary>Raw Strategy5 FastAPI Payload</summary>
          <pre class="json-box">{{ JSON.stringify({ runtime: strategy5Runtime, evidence: strategy5Evidence }, null, 2) }}</pre>
        </details>
      </section>

      <section v-else-if="activePage === 'strategy6'">
        <PageHeader title="Strategy6 / 异动陆号" subtitle="市场接受入场支线：WAIT 后进入常驻观察，等待方向被市场接受、价格变合适，再进入 paper 对比。" />
        <div class="grid-kpi">
          <article class="card kpi"><div class="kpi-label">Runtime</div><div class="kpi-value" :class="statusClass(strategy6Summary.status)">{{ strategy6Summary.status }}</div><div class="kpi-sub">latest plan {{ strategy6Summary.generated_at }}</div></article>
          <article class="card kpi"><div class="kpi-label">Plans</div><div class="kpi-value">{{ strategy6Summary.count }}</div><div class="kpi-sub">strategy6 trade plan rows</div></article>
          <article class="card kpi"><div class="kpi-label">Executable</div><div class="kpi-value up">{{ strategy6Summary.executable }}</div><div class="kpi-sub">paper consumable if accepted</div></article>
          <article class="card kpi"><div class="kpi-label">Wait Pool</div><div class="kpi-value flat">{{ strategy6Summary.wait }}</div><div class="kpi-sub">WAIT_CONFIRM / WAIT_REBOUND</div></article>
          <article class="card kpi"><div class="kpi-label">Attempts</div><div class="kpi-value flat">{{ strategy6Summary.attempts }}</div><div class="kpi-sub">persistent recheck ledger</div></article>
          <article class="card kpi"><div class="kpi-label">Health</div><div class="kpi-value" :class="statusClass(strategy6Daemon.health_status || strategy6Daemon.status)">{{ strategy6Daemon.health_status || strategy6Daemon.status || '-' }}</div><div class="kpi-sub">heartbeat {{ strategy6Daemon.heartbeat_age_sec ?? '-' }}s / {{ strategy6Daemon.stale_after_sec ?? '-' }}s</div></article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Daemon Controls</div>
            <span class="tag blue">{{ strategy6Daemon.pool_count || 0 }} pool · {{ strategy6Daemon.due_count || 0 }} due</span>
          </div>
          <div class="panel-body controls-row">
            <button class="btn primary" :disabled="loading" @click="strategy6DaemonAction('recheck')">Recheck Now</button>
            <button class="btn" :disabled="loading" @click="strategy6DaemonAction('start')">Start Daemon</button>
            <button class="btn danger" :disabled="loading" @click="strategy6DaemonAction('stop')">Stop Daemon</button>
            <button class="btn" :disabled="loading" @click="strategy6DaemonAction('watchdog')">Watchdog Check</button>
            <button class="btn warn" :disabled="loading" @click="strategy6DaemonAction('recover')">Recover</button>
            <span class="tag" :class="statusClass(strategy6Daemon.status)">{{ strategy6Daemon.status || 'unknown' }}</span>
            <span class="tag" :class="statusClass(strategy6WatchdogStatus)">watchdog {{ strategy6WatchdogStatus }}</span>
            <span class="tag" :class="strategy6Daemon.pid_alive ? 'green' : 'yellow'">pid {{ strategy6Daemon.pid_alive ? 'alive' : 'not alive' }}</span>
            <span class="muted">next {{ strategy6Daemon.next_check_at || '-' }}</span>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Decision Ledger</div>
            <span class="tag blue">{{ strategy6DecisionRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Symbol</th><th>Legacy</th><th>State</th><th>Wait</th><th>Executable</th><th>Reasons</th></tr></thead>
              <tbody>
                <tr v-for="row in strategy6DecisionRows.slice(0, 200)" :key="row.evidence_id || row.symbol">
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td>{{ row.legacy_side || "-" }}</td>
                  <td><span class="tag" :class="statusClass(row.decision_state)">{{ row.decision_state || "-" }}</span></td>
                  <td>{{ row.wait_state || "-" }}</td>
                  <td><span class="tag" :class="row.executable ? 'good' : 'warn'">{{ row.executable ? "yes" : "no" }}</span></td>
                  <td class="reason-cell">{{ (row.reason_codes || []).join(", ") || "-" }}</td>
                </tr>
                <tr v-if="!strategy6DecisionRows.length"><td colspan="6" class="empty-state">No Strategy6 decisions yet. Run pipeline with strategy6 selected.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Market Acceptance Evidence</div>
            <span class="tag blue">{{ strategy6EvidenceRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Symbol</th><th>Side</th><th>Direction</th><th>Price</th><th>Market</th><th>Range</th><th>Spread</th><th>State</th></tr></thead>
              <tbody>
                <tr v-for="row in strategy6EvidenceRows.slice(0, 200)" :key="row.evidence_id || `${row.symbol}-${row.generated_at}`">
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td>{{ row.legacy_side || "-" }}</td>
                  <td>{{ row.direction_acceptance_score ?? "-" }}</td>
                  <td>{{ row.entry_price_quality_score ?? "-" }}</td>
                  <td>{{ row.market_acceptance_score ?? "-" }}</td>
                  <td>{{ row.range_pos ?? "-" }}</td>
                  <td>{{ row.spread_bps ?? "-" }}</td>
                  <td><span class="tag" :class="statusClass(row.decision_state)">{{ row.decision_state || "-" }}</span></td>
                </tr>
                <tr v-if="!strategy6EvidenceRows.length"><td colspan="8" class="empty-state">No Strategy6 evidence yet.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Wait Pool</div>
            <span class="tag blue">{{ strategy6WaitRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Symbol</th><th>Status</th><th>Side</th><th>Wait</th><th>Attempts</th><th>Next</th><th>Direction</th><th>Price</th><th>Market</th><th>Reasons</th></tr></thead>
              <tbody>
                <tr v-for="row in strategy6WaitRows.slice(0, 200)" :key="row.pool_id || row.evidence_id || row.symbol">
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td><span class="tag" :class="statusClass(row.status)">{{ row.status || "-" }}</span></td>
                  <td>{{ row.current_side || row.legacy_side || "-" }}</td>
                  <td>{{ row.wait_state || "-" }}</td>
                  <td>{{ row.attempts ?? "-" }}</td>
                  <td>{{ row.next_check_at || "-" }}</td>
                  <td>{{ row.direction_acceptance_score ?? "-" }}</td>
                  <td>{{ row.entry_price_quality_score ?? "-" }}</td>
                  <td>{{ row.market_acceptance_score ?? "-" }}</td>
                  <td class="reason-cell">{{ (row.reason_codes || []).join(", ") || "-" }}</td>
                </tr>
                <tr v-if="!strategy6WaitRows.length"><td colspan="10" class="empty-state">No Strategy6 wait rows yet.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Attempt Timeline</div>
            <span class="tag blue">{{ strategy6AttemptRows.length }} rows</span>
          </div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Time</th><th>Symbol</th><th>Attempt</th><th>State</th><th>Wait</th><th>Action</th><th>Executable</th><th>Direction</th><th>Price</th><th>Market</th><th>Reasons</th></tr></thead>
              <tbody>
                <tr v-for="row in strategy6AttemptRows.slice(0, 200)" :key="row.attempt_id">
                  <td>{{ row.checked_at || "-" }}</td>
                  <td><strong>{{ row.symbol }}</strong></td>
                  <td>{{ row.attempt_no ?? "-" }}</td>
                  <td><span class="tag" :class="statusClass(row.decision_state)">{{ row.decision_state || "-" }}</span></td>
                  <td>{{ row.wait_state || "-" }}</td>
                  <td>{{ row.action || "-" }}</td>
                  <td>{{ row.executable ? "yes" : "no" }}</td>
                  <td>{{ row.direction_acceptance_score ?? "-" }}</td>
                  <td>{{ row.entry_price_quality_score ?? "-" }}</td>
                  <td>{{ row.market_acceptance_score ?? "-" }}</td>
                  <td class="reason-cell">{{ (row.reason_codes || []).join(", ") || "-" }}</td>
                </tr>
                <tr v-if="!strategy6AttemptRows.length"><td colspan="11" class="empty-state">No Strategy6 observe attempts yet.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <details class="card panel debug-details">
          <summary>Raw Strategy6 FastAPI Payload</summary>
          <pre class="json-box">{{ JSON.stringify({ runtime: strategy6Runtime, evidence: strategy6Evidence, decisions: strategy6Decisions, wait_pool: strategy6WaitPool, attempts: strategy6Attempts, heartbeat: strategy6Heartbeat, watchdog: strategy6Watchdog }, null, 2) }}</pre>
        </details>
      </section>

      <section v-else-if="activePage === 'audit'">
        <PageHeader title="Run-Level Audit" subtitle="以 run_id 为单位审计公共上游、三条策略线、每个 symbol 的 micro / refresh / trade plan / paper / Feishu 链条。" />

        <div class="audit-kpi-grid">
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Run ID</span><span class="status-icon"><ShieldCheck /></span></div>
            <div class="kpi-value">{{ runAudit.run_id || "-" }}</div>
            <div class="kpi-sub">{{ runAudit.cycle_id || "cycle pending" }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Audit Status</span><span class="status-icon"><FileWarning /></span></div>
            <div class="kpi-value" :class="runAuditStatusClass">{{ runAudit.status || "-" }}</div>
            <div class="kpi-sub">tech {{ runAudit.technical_failure_count || runAudit.failure_count || 0 }} / business {{ runAudit.business_warning_count || 0 }} / warn {{ runAudit.warning_count || 0 }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Symbols</span><span class="status-icon"><ListChecks /></span></div>
            <div class="kpi-value">{{ runAudit.summary?.symbol_row_count || runAuditSymbols.length || 0 }}</div>
            <div class="kpi-sub">candidate {{ runAudit.summary?.candidate_symbol_count || 0 }}</div>
          </article>
          <article class="card kpi">
            <div class="kpi-top"><span class="kpi-label">Downstream</span><span class="status-icon"><Bell /></span></div>
            <div class="kpi-value">{{ runAudit.downstream?.paper?.order_count || 0 }} / {{ runAudit.downstream?.feishu?.delivery_count || 0 }}</div>
            <div class="kpi-sub">paper orders / Feishu · settlement {{ runAudit.summary?.paper_settlement_status || runAudit.paper_settlement?.status || "-" }}</div>
          </article>
        </div>

        <article class="card audit-compact-panel audit-run-selector">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Run Selector</div>
            <div class="tag-row">
              <span class="tag" :class="runAuditStatusClass">{{ runAudit.status || "-" }}</span>
              <span class="tag" :class="auditLazyStatusClass">{{ auditLazyStatusText }}</span>
            </div>
          </div>
          <div class="panel-body audit-selector-body">
            <button class="small-action" :disabled="!previousRunId" @click="openRunAudit(previousRunId)">Previous</button>
            <button class="small-action" :disabled="!nextRunId" @click="openRunAudit(nextRunId)">Next</button>
            <button class="small-action primary" @click="openLatestRunAudit">Latest</button>
            <button class="small-action" :disabled="!runAudit.run_id || auditSideLoading" @click="requestAuditDetails">
              {{ auditSideLoading ? "Loading Details" : (auditDetailsReady ? "Reload Details" : "Load Detail Panels") }}
            </button>
            <button class="small-action" :disabled="!runAudit.run_id" @click="toggleAuditRawPayload">
              {{ showAuditRawPayload ? "Hide Raw Payload" : "Show Raw Payload" }}
            </button>
            <select class="audit-run-select" :value="runAudit.run_id || ''" @change="openRunAudit($event.target.value)">
              <option v-for="row in runAuditRuns" :key="`select-${row.run_id}`" :value="row.run_id">
                {{ row.run_id }} · {{ row.status }} · {{ row.generated_at }}
              </option>
            </select>
            <span class="path">selected {{ runAudit.run_id || "-" }} / {{ runAudit.cycle_id || "-" }}</span>
          </div>
          <div v-if="auditSideLoading || auditLazyError || (!auditDetailsRequested && runAudit.run_id)" class="audit-lazy-note">
            <span>{{ auditSideLoading ? "Loading micro/evidence detail panels." : (auditLazyError || "Detail panels are lazy-loaded. Click Load Detail Panels when needed.") }}</span>
            <span class="path">core run audit remains usable while details load</span>
          </div>
        </article>

        <article class="card audit-compact-panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Paper Consumption</div>
            <span class="tag" :class="statusClass(paperConsumption.status)">{{ paperConsumption.status || "-" }}</span>
          </div>
          <div class="panel-body">
            <div class="audit-line-metrics">
              <div class="audit-metric-box">
                <div class="plan-name">executable</div>
                <div class="plan-num">{{ paperConsumption.executable_count || 0 }}</div>
                <div class="plan-meta"><span>run</span><b>{{ paperConsumption.run_id || "-" }}</b></div>
              </div>
              <div class="audit-metric-box">
                <div class="plan-name">orders</div>
                <div class="plan-num up">{{ paperConsumption.order_count || 0 }}</div>
                <div class="plan-meta"><span>skips</span><b>{{ paperConsumption.skip_count || 0 }}</b></div>
              </div>
              <div class="audit-metric-box">
                <div class="plan-name">missing</div>
                <div class="plan-num" :class="statusClass((paperConsumption.missing_count || 0) ? 'failed' : 'pass')">{{ paperConsumption.missing_count || 0 }}</div>
                <div class="plan-meta"><span>reason</span><b>{{ paperConsumption.paper_run_once_reason || "-" }}</b></div>
              </div>
            </div>
            <div class="audit-mini-steps">
              <div class="audit-step-row">
                <span><b>paper_run_once</b><small>{{ (paperConsumption.paper_run_once_reason_codes || []).join(", ") || "no reason codes" }}</small></span>
                <span class="tag" :class="statusClass(paperConsumption.paper_run_once_status)">{{ paperConsumption.paper_run_once_status || "-" }}</span>
              </div>
              <div class="audit-step-row">
                <span><b>tick_lock</b><small>{{ paperConsumption.paper_tick_lock?.path || "-" }}</small></span>
                <span class="tag" :class="statusClass(paperConsumption.paper_tick_lock?.status || (paperConsumption.paper_tick_lock?.exists ? 'busy' : 'clear'))">
                  {{ paperConsumption.paper_tick_lock?.status || (paperConsumption.paper_tick_lock?.exists ? "busy" : "clear") }}
                </span>
              </div>
              <div class="audit-step-row">
                <span><b>tick_lock reconcile</b><small>{{ paperConsumption.paper_tick_lock?.reconcile_action || "-" }}</small></span>
                <span class="tag">{{ paperConsumption.paper_tick_lock?.retry_count ?? 0 }} retries</span>
              </div>
              <div class="audit-step-row">
                <span><b>inline wakeup retry</b><small>{{ paperConsumption.paper_run_once_inline_retry?.first_reason || "-" }}</small></span>
                <span class="tag">{{ paperConsumption.paper_run_once_inline_retry?.attempts ?? 0 }} attempts</span>
              </div>
              <div v-for="(symbols, line) in paperConsumption.missing_by_line || {}" :key="`paper-missing-${line}`" class="audit-step-row">
                <span><b>{{ line }}</b><small>{{ (symbols || []).join(", ") || "none" }}</small></span>
                <span class="tag" :class="statusClass((symbols || []).length ? 'failed' : 'pass')">{{ (symbols || []).length ? "missing" : "settled" }}</span>
              </div>
            </div>
          </div>
        </article>

        <article class="card audit-compact-panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Strategy4 Sidecar Evidence</div>
            <div class="tag-row">
              <span class="tag blue">sidecar</span>
              <span class="tag" :class="statusClass(auditStrategy4Sidecar.daemon_state)">{{ auditStrategy4Sidecar.daemon_state || "-" }}</span>
            </div>
          </div>
          <div class="panel-body">
            <div v-if="auditStrategy4SidecarFallback" class="notice warn">
              latest sidecar evidence, not selected-run evidence
            </div>
            <div class="audit-scope-note">
              <span>Strategy4 is an observe daemon. It is not a selected pipeline line.</span>
              <span class="path">source {{ auditStrategy4Sidecar.source_line || "without_micro" }} · paper consumes MARKET executable only</span>
            </div>
            <div class="audit-line-metrics">
              <div class="audit-metric-box">
                <div class="plan-name">pool</div>
                <div class="plan-num">{{ auditStrategy4Sidecar.pool_count || 0 }}</div>
                <div class="plan-meta"><span>still wait</span><b>{{ auditStrategy4StatusCounts.still_wait || 0 }}</b></div>
              </div>
              <div class="audit-metric-box">
                <div class="plan-name">attempts</div>
                <div class="plan-num">{{ auditStrategy4Sidecar.attempt_count || 0 }}</div>
                <div class="plan-meta"><span>exec attempts</span><b>{{ auditStrategy4Sidecar.attempt_executable_count || 0 }}</b></div>
              </div>
              <div class="audit-metric-box">
                <div class="plan-name">latest plan</div>
                <div class="plan-num">{{ auditStrategy4LatestPlan.count || 0 }} / {{ auditStrategy4LatestPlan.executable_count || 0 }}</div>
                <div class="plan-meta"><span>fresh</span><b>{{ auditStrategy4LatestPlan.output_fresh ? "yes" : (auditStrategy4LatestPlan.stale_output_reason || "no") }}</b></div>
              </div>
              <div class="audit-metric-box">
                <div class="plan-name">downstream</div>
                <div class="plan-num up">{{ auditStrategy4Downstream.paper_orders || 0 }}</div>
                <div class="plan-meta"><span>TQ samples</span><b>{{ auditStrategy4Downstream.trade_quality_samples || 0 }}</b></div>
              </div>
            </div>
            <div class="audit-mini-steps">
              <div class="audit-step-row">
                <span><b>latest_trade_plan_strategy4</b><small>{{ auditStrategy4LatestPlan.output_run_id || "-" }} / {{ auditStrategy4LatestPlan.output_cycle_id || "-" }}</small></span>
                <span class="tag" :class="statusClass(auditStrategy4LatestPlan.output_fresh ? 'ok' : 'warn')">{{ auditStrategy4LatestPlan.output_fresh ? "fresh" : "sidecar latest" }}</span>
              </div>
              <div class="audit-step-row">
                <span><b>pool status</b><small>{{ JSON.stringify(auditStrategy4StatusCounts) }}</small></span>
                <span class="tag blue">observe</span>
              </div>
              <div class="audit-step-row">
                <span><b>paper / skips / closed</b><small>{{ auditStrategy4Downstream.paper_orders || 0 }} / {{ auditStrategy4Downstream.paper_skips || 0 }} / {{ auditStrategy4Downstream.paper_closed || 0 }}</small></span>
                <span class="tag" :class="statusClass((auditStrategy4LatestPlan.executable_count || 0) ? 'ok' : 'warn')">
                  {{ (auditStrategy4LatestPlan.executable_count || 0) ? "executable present" : "no executable yet" }}
                </span>
              </div>
              <div class="audit-step-row">
                <span><b>reason codes</b><small>{{ (auditStrategy4Sidecar.reason_codes || []).join(", ") || "normal sidecar evidence" }}</small></span>
                <span class="tag" :class="statusClass((auditStrategy4Sidecar.reason_codes || []).length ? 'warn' : 'ok')">{{ (auditStrategy4Sidecar.reason_codes || []).length ? "check" : "ok" }}</span>
              </div>
            </div>
          </div>
        </article>

        <div class="audit-line-summary-grid">
          <article v-for="line in pipelineStrategyLineOrder" :key="line" class="card audit-compact-panel audit-line-card">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>{{ lineLabel(line) }}</div>
              <div class="tag-row">
                <span class="tag" :class="statusClass(lineSelectedLabel(line))">{{ lineSelectedLabel(line) }}</span>
                <span class="tag" :class="statusClass(runAuditLines[line]?.status)">{{ runAuditLines[line]?.status || runAudit.summary?.line_status?.[line] || "-" }}</span>
              </div>
            </div>
            <div class="panel-body">
              <div class="audit-line-metrics">
                <div class="audit-metric-box"><div class="plan-name">symbols</div><div class="plan-num">{{ lineSymbolsCount(line) }}</div><div class="plan-meta"><span>lineage</span><b>{{ runAuditLines[line]?.run_id === runAudit.run_id ? "match" : "check" }}</b></div></div>
                <div class="audit-metric-box"><div class="plan-name">plans</div><div class="plan-num">{{ runAuditLines[line]?.count || 0 }}</div><div class="plan-meta"><span>actions</span><b>{{ lineActionText(line) }}</b></div></div>
                <div class="audit-metric-box"><div class="plan-name">executable</div><div class="plan-num up">{{ runAuditLines[line]?.executable_count ?? runAudit.summary?.executable_count?.[line] ?? 0 }}</div><div class="plan-meta"><span>pipeline</span><b>{{ lineStageText(line) }}</b></div></div>
              </div>
              <div class="audit-mini-steps">
                <div v-for="step in runAuditLines[line]?.steps || []" :key="step.name" class="audit-step-row">
                  <span><b>{{ step.name }}</b><small>{{ auditStepDetailText(step) }}</small></span>
                  <span class="tag" :class="statusClass(step.status)">{{ step.status }}</span>
                </div>
                <div v-if="!(runAuditLines[line]?.steps || []).length" class="audit-step-row">
                  <span>No line audit steps</span>
                  <span class="tag warn">empty</span>
                </div>
              </div>
            </div>
          </article>
        </div>

        <div class="wide-grid audit-wide">
          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Run History</div>
              <span class="tag blue">{{ runAuditList.source || "latest" }}</span>
            </div>
            <div class="panel-body audit-run-list">
              <button
                v-for="row in runAuditRuns"
                :key="row.run_id"
                class="audit-run-card"
                :class="{ active: row.run_id === runAudit.run_id }"
                @click="openRunAudit(row.run_id)"
              >
                <strong>{{ row.run_id }}</strong>
                <span>{{ row.cycle_id }}</span>
                <b :class="statusClass(row.status)">{{ row.status }}</b>
                <small>{{ row.generated_at }}</small>
              </button>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Shared Upstream</div>
              <span class="tag" :class="runAuditStatusClass">contract</span>
            </div>
            <div class="panel-body">
              <div v-for="step in runAudit.shared_steps || []" :key="step.name" class="audit-row">
                <div class="audit-code">{{ step.severity || "info" }}</div>
                <div class="audit-name">{{ step.name }}</div>
                <span class="tag" :class="statusClass(step.status)">{{ step.status }}</span>
              </div>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>REST / Step1.5 Health</div>
              <span class="tag" :class="statusClass(auditStep15Health.rest_circuit_state)">{{ auditStep15Health.rest_circuit_state }}</span>
            </div>
            <div class="panel-body">
              <div class="audit-scope-note">
                <span>{{ auditStep15Health.scope }}</span>
                <span class="path">{{ auditStep15Health.generated_at }}</span>
              </div>
              <div class="plan-grid audit-health-grid">
                <div class="plan-box">
                  <div class="plan-name">exchangeInfo</div>
                  <div class="plan-num">{{ auditStep15Health.exchange_info_source }}</div>
                  <div class="plan-meta"><span>cache age</span><b>{{ secondsText(auditStep15Health.exchange_info_cache_age_sec) }}</b></div>
                </div>
                <div class="plan-box">
                  <div class="plan-name">warmup</div>
                  <div class="plan-num" :class="statusClass(auditStep15Health.ready_status_detail)">{{ auditStep15Health.ready_status_detail }}</div>
                  <div class="plan-meta"><span>usable</span><b>{{ auditStep15Health.usable_symbol_count }}</b></div>
                </div>
                <div class="plan-box">
                  <div class="plan-name">snapshot</div>
                  <div class="plan-num" :class="statusClass(auditStep15Health.snapshot_status)">{{ auditStep15Health.snapshot_status }}</div>
                  <div class="plan-meta"><span>allowed</span><b>{{ auditStep15Health.candidate_allowed_count }}</b></div>
                </div>
                <div class="plan-box">
                  <div class="plan-name">REST budget</div>
                  <div class="plan-num" :class="statusClass(auditStep15Health.rest_budget_state)">{{ auditStep15Health.rest_budget_state }}</div>
                  <div class="plan-meta"><span>requests</span><b>{{ auditStep15Health.rest_request_count }}</b></div>
                </div>
                <div class="plan-box">
                  <div class="plan-name">recovery</div>
                  <div class="plan-num" :class="statusClass(auditStep15Health.rest_recovery_stage)">{{ auditStep15Health.rest_recovery_stage }}</div>
                  <div class="plan-meta"><span>shard</span><b>{{ auditStep15Health.current_shard_size ?? "-" }} -> {{ auditStep15Health.next_shard_size ?? "-" }}</b></div>
                </div>
                <div class="plan-box">
                  <div class="plan-name">fresh / usable / blocked</div>
                  <div class="plan-num">{{ auditStep15Health.fresh_count }} / {{ auditStep15Health.stale_usable_count }} / {{ auditStep15Health.stale_blocked_count }}</div>
                  <div class="plan-meta"><span>blocked</span><b>{{ auditStep15Health.blocked_symbol_count }}</b></div>
                </div>
              </div>
              <div class="notice-list">
                <div class="notice">
                  <span>live REST</span>
                  <span class="tag" :class="statusClass(auditStep15Health.live_rest_allowed ? 'pass' : 'blocked')">{{ auditStep15Health.live_rest_allowed ? "allowed" : "blocked" }}</span>
                </div>
                <div class="notice">
                  <span>market source / freshness</span>
                  <span class="tag" :class="statusClass(auditStep15Health.market_snapshot_freshness_tier)">{{ auditStep15Health.market_snapshot_source }} · {{ auditStep15Health.market_snapshot_freshness_tier }} · {{ secondsText(auditStep15Health.market_snapshot_age_sec) }}</span>
                </div>
                <div class="notice">
                  <span>REST status codes</span>
                  <span class="path">{{ JSON.stringify(auditStep15Health.rest_status_code_counts) }} · 418 {{ auditStep15Health.status_418_count }} / 429 {{ auditStep15Health.status_429_count }}</span>
                </div>
                <div class="notice">
                  <span>daemon / watchdog</span>
                  <span class="tag" :class="statusClass(auditStep15Health.watchdog_status)">{{ auditStep15Health.daemon_status }} · {{ auditStep15Health.watchdog_status }} · heartbeat {{ secondsText(auditStep15Health.heartbeat_age_sec) }}</span>
                </div>
                <div class="notice">
                  <span>source mix</span>
                  <span class="path">{{ auditStep15SourceMixRows.map(([key, value]) => `${key}:${value}`).join(" / ") || "-" }}</span>
                </div>
                <div class="notice">
                  <span>skipped symbols</span>
                  <span class="path">{{ auditStep15Health.skipped_symbol_count }} / {{ auditStep15Health.skipped_symbols.slice(0, 8).join(", ") || "-" }}</span>
                </div>
                <div class="notice">
                  <span>reason codes</span>
                  <span class="path">{{ auditStep15Health.reason_codes.join(", ") || "-" }}</span>
                </div>
              </div>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Candidate Governance</div>
              <span class="tag blue">{{ candidateGovernanceRows.length }} symbols</span>
            </div>
            <div class="panel-body">
              <div class="audit-scope-note">
                <span>{{ candidateGovernanceScopeLabel }}</span>
                <span class="path">generated {{ candidateGovernanceGeneratedAt }} · universe {{ candidateGovernanceUniverseAt }} · snapshot {{ candidateGovernanceSnapshotAt }}</span>
              </div>
              <div class="plan-grid">
                <div class="plan-box"><div class="plan-name">business pools</div><div class="plan-num">{{ Object.keys(candidateGovernanceCounts.business_pool || {}).length }}</div></div>
                <div class="plan-box"><div class="plan-name">quality tiers</div><div class="plan-num">{{ Object.keys(candidateGovernanceCounts.trade_quality_tier || {}).length }}</div></div>
                <div class="plan-box"><div class="plan-name">execution tiers</div><div class="plan-num">{{ Object.keys(candidateGovernanceCounts.execution_tier || {}).length }}</div></div>
                <div class="plan-box"><div class="plan-name">hydration gaps</div><div class="plan-num">{{ candidateGovernanceCounts.profile_hydration_status?.incomplete || 0 }}</div></div>
              </div>
              <div class="audit-symbol-table candidate-governance-table compact-table">
                <table>
                  <thead><tr><th>Symbol</th><th>Business</th><th>Hydration</th><th>Exec</th><th>Quality</th><th>Market</th><th>HF Stop</th><th>Slip Risk</th><th>Source</th><th>Universe At</th><th>Snapshot At</th></tr></thead>
                  <tbody>
                    <tr v-for="row in candidateGovernanceRows.slice(0, 80)" :key="`gov-${row.symbol}`">
                      <td>{{ row.symbol }}</td>
                      <td>{{ row.universe_profile?.business_pool || "-" }}</td>
                      <td><span class="tag" :class="statusClass(row.profile_hydration?.status)">{{ row.profile_hydration?.status || "-" }}</span></td>
                      <td>{{ row.risk_profile?.execution_tier || "-" }}</td>
                      <td><span class="tag" :class="statusClass(row.tradability_profile?.trade_quality_tier)">{{ row.tradability_profile?.trade_quality_tier || "-" }}</span></td>
                      <td>{{ row.tradability_profile?.market_entry_score ?? "-" }}</td>
                      <td>{{ row.tradability_profile?.hf_stop_score ?? "-" }}</td>
                      <td>{{ row.tradability_profile?.slippage_risk_score ?? "-" }}</td>
                      <td>{{ row.item_snapshot_source || row.snapshot_source_priority || row.source || "-" }}</td>
                      <td class="path">{{ row.universe_generated_at || "-" }}</td>
                      <td class="path">{{ row.light_snapshot_generated_at || row.last_live_refresh_at || "-" }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div v-if="!candidateGovernanceRows.length" class="notice"><span>No candidate governance data</span><span class="tag warn">empty</span></div>
            </div>
          </article>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Data Quality Attribution</div>
            <span class="tag" :class="statusClass(microQualityFreshness)">{{ microQualityFreshness }} · {{ microQualityRows.length }} rows</span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">technical fix</div><div class="plan-num flat">{{ microQualityCount("technical_fix") }}</div></div>
              <div class="plan-box"><div class="plan-name">market accept</div><div class="plan-num up">{{ microQualityCount("market_accept") }}</div></div>
              <div class="plan-box"><div class="plan-name">unknown</div><div class="plan-num">{{ microQualityCount("unknown_blocker") }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Line</th><th>Symbol</th><th>State</th><th>Raw Reason</th><th>Attribution</th><th>Category</th><th>CVD Age</th><th>OFI Age</th><th>Lag</th><th>Lag Side</th><th>Processed</th><th>Driver</th><th>Action</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microQualityRows.slice(0, 200)" :key="`${microQualityLine(row)}-${row.symbol}-${microQualityRawReason(row)}`">
                    <td>{{ lineLabel(microQualityLine(row)) }}</td>
                    <td>{{ row.symbol || "-" }}</td>
                    <td>{{ row.state || "-" }}</td>
                    <td class="path">{{ microQualityRawReason(row) }}</td>
                    <td class="path">{{ microQualityAttribution(row) }}</td>
                    <td><span class="tag" :class="statusClass(microQualityCategory(row))">{{ microQualityCategory(row) }}</span></td>
                    <td>{{ microQualityEvidence(row, "cvd_update_age_sec") }}</td>
                    <td>{{ microQualityEvidence(row, "ofi_update_age_sec") }}</td>
                    <td>{{ microQualityEvidence(row, "ofi_cvd_lag_sec") }}</td>
                    <td>{{ microQualityEvidence(row, "ofi_cvd_lag_side") }}</td>
                    <td>{{ microQualityEvidence(row, "last_processed_bucket_ts_sec") }}</td>
                    <td class="path">{{ microQualityDriverMetrics(row) }}</td>
                    <td class="path">{{ microQualityAction(row) }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microQualityRows.length" class="notice"><span>No micro data quality rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Evidence Runtime V2</div>
            <span class="tag" :class="statusClass(microEvidenceFreshness)">{{ microEvidenceFreshness }} 路 {{ microEvidenceRows.length }} symbols</span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">P0 runtime</div><div class="plan-num down">{{ microEvidenceCount("P0") }}</div></div>
              <div class="plan-box"><div class="plan-name">P1 data</div><div class="plan-num flat">{{ microEvidenceCount("P1") }}</div></div>
              <div class="plan-box"><div class="plan-name">P2 warmup</div><div class="plan-num">{{ microEvidenceCount("P2") }}</div></div>
            </div>
            <div class="plan-grid">
              <div class="plan-box" v-for="row in microTargetDistributionRows.slice(0, 4)" :key="`target-src-${row.name}`">
                <div class="plan-name">{{ row.name }}</div>
                <div class="plan-num">{{ row.count }}</div>
              </div>
              <div v-if="!microTargetDistributionRows.length" class="notice"><span>No target source distribution</span><span class="tag warn">empty</span></div>
            </div>
            <div class="audit-mini-steps reason-funnel">
              <div v-for="row in microTargetRows.slice(0, 10)" :key="`target-row-${row.tier}-${row.symbol}`" class="notice">
                <span class="path">{{ row.symbol }}</span>
                <span>{{ row.tier }} / {{ row.source_state }}</span>
                <span class="tag blue">{{ row.retained_reason || row.sticky_source || "current" }}</span>
                <small class="path">age {{ row.sticky_age_sec ?? "-" }}s / cycle {{ row.sticky_cycle_count ?? "-" }}</small>
              </div>
            </div>
            <div class="audit-mini-steps reason-funnel">
              <div v-for="row in microEvidenceReasonFunnel" :key="row.reason" class="notice">
                <span class="path">{{ row.reason }}</span>
                <span>{{ row.count }} rows / {{ row.symbols }} symbols</span>
                <span class="tag" :class="row.p0 ? 'bad' : 'warn'">{{ row.p0 ? `${row.p0} P0` : 'non-P0' }}</span>
                <small class="path">{{ row.attributed || "-" }}</small>
              </div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Line</th><th>Symbol</th><th>Status</th><th>Severity</th><th>CVD Class</th><th>Agg Gap</th><th>OFI Gap</th><th>Backpressure</th><th>History</th><th>Store</th><th>Store Ratio</th><th>aggTrade</th><th>Book</th><th>Depth</th><th>Barrier</th><th>Align</th><th>Lag</th><th>Z Window</th><th>Z Reason</th><th>Reasons</th><th>Action</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microEvidenceRows.slice(0, 220)" :key="`v2-${row.strategy_line}-${row.symbol}`">
                    <td>{{ lineLabel(row.strategy_line || row.line) }}</td>
                    <td>{{ row.symbol || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.status)">{{ row.status || "-" }}</span></td>
                    <td><span class="tag" :class="statusClass(row.severity)">{{ row.severity || "-" }}</span></td>
                    <td>{{ microEvidenceRuntimeValue(row, "runtime_evidence.cvd_runtime.never_updated_class") }}</td>
                    <td><span class="tag" :class="statusClass(microEvidenceRuntimeValue(row, 'runtime_evidence.aggtrade_runtime.bucket_gap_class'))">{{ microEvidenceRuntimeValue(row, "runtime_evidence.aggtrade_runtime.bucket_gap_class") }}</span></td>
                    <td><span class="tag" :class="statusClass(microEvidenceRuntimeValue(row, 'runtime_evidence.book_depth_runtime.ofi_gap_class'))">{{ microEvidenceRuntimeValue(row, "runtime_evidence.book_depth_runtime.ofi_gap_class") }}</span></td>
                    <td><span class="tag" :class="statusClass(microEvidenceRuntimeValue(row, 'runtime_evidence.book_depth_runtime.queue_backpressure_state'))">{{ microEvidenceRuntimeValue(row, "runtime_evidence.book_depth_runtime.queue_backpressure_state") }}</span></td>
                    <td>{{ microEvidenceRuntimeValue(row, "runtime_evidence.z_history_runtime.history_gap_class") }}</td>
                    <td>{{ microEvidenceStoreWindow(row, "full_z_status") }}</td>
                    <td>{{ microEvidenceStoreWindow(row, "valid_bucket_ratio") }}</td>
                    <td>{{ microEvidenceCoverage(row, "aggTrade") }}</td>
                    <td>{{ microEvidenceCoverage(row, "bookTicker") }}</td>
                    <td>{{ microEvidenceCoverage(row, "partialDepth5") }}</td>
                    <td>{{ microEvidenceFrame(row, "commit_barrier_status") }}</td>
                    <td>{{ microEvidenceRuntimeValue(row, "runtime_evidence.bucket_alignment.true_alignment_reason") }}</td>
                    <td>{{ microEvidenceFrame(row, "ofi_cvd_lag_bucket_sec") }}</td>
                    <td>{{ microEvidenceFrame(row, "z_window_count") }}/{{ microEvidenceFrame(row, "z_window_required_count") }}</td>
                    <td>{{ microEvidenceFrame(row, "missing_reason") }}</td>
                    <td class="path">{{ (row.raw_reasons || []).join(", ") }}</td>
                    <td class="path">{{ (row.recommended_actions || []).slice(0, 2).join(" / ") }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microEvidenceRows.length" class="notice"><span>No runtime V2 evidence rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Fast Runtime Stability</div>
            <span class="tag" :class="statusClass(microFastRuntimeRows.length ? 'ok' : 'warn')">
              {{ microFastRuntimeRows.length }} micro_fast rows
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">barrier pass</div><div class="plan-num up">{{ microFastRuntimeAudit.summary?.bucket_commit_barrier_counts?.pass || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">barrier failed</div><div class="plan-num down">{{ microFastRuntimeAudit.summary?.bucket_commit_barrier_counts?.failed || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">aligned pass</div><div class="plan-num up">{{ microFastRuntimeAudit.summary?.aligned_frame_gate_counts?.pass || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">z ready</div><div class="plan-num">{{ microFastRuntimeAudit.summary?.fast_z_continuity_counts?.ready || 0 }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>Status</th><th>Barrier</th><th>Failed Stage</th><th>Aligned Gate</th><th>Lag</th><th>Fast-Z</th><th>aggTrade</th><th>Book</th><th>Depth</th><th>Reasons</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microFastRuntimeRows.slice(0, 200)" :key="`fast-runtime-${row.run_id}-${row.symbol}`">
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.status)">{{ row.status || "-" }}</span></td>
                    <td><span class="tag" :class="statusClass(row.bucket_commit_barrier?.barrier_status)">{{ row.bucket_commit_barrier?.barrier_status || "-" }}</span></td>
                    <td>{{ row.bucket_commit_barrier?.failed_stage || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.aligned_frame_gate?.aligned_frame_pass ? 'pass' : row.aligned_frame_gate?.block_reason)">{{ row.aligned_frame_gate?.aligned_frame_pass ? "pass" : (row.aligned_frame_gate?.block_reason || "-") }}</span></td>
                    <td>{{ row.aligned_frame_gate?.lag_bucket_sec ?? "-" }}</td>
                    <td>{{ row.fast_z_continuity?.continuity_status || "-" }} {{ row.fast_z_continuity?.z_window_count ?? "-" }}/{{ row.fast_z_continuity?.z_window_required_count ?? "-" }}</td>
                    <td>{{ row.coverage_root_cause_v2?.aggTrade?.coverage_class || "-" }}</td>
                    <td>{{ row.coverage_root_cause_v2?.bookTicker?.coverage_class || "-" }}</td>
                    <td>{{ row.coverage_root_cause_v2?.partialDepth5?.coverage_class || "-" }}</td>
                    <td class="path">{{ (row.raw_reasons || []).slice(0, 5).join(", ") }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microFastRuntimeRows.length" class="notice"><span>No micro_fast runtime stability rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Fast Tail Cleanup</div>
            <span class="tag" :class="statusClass(microFastTailCleanupRows.length ? 'ok' : 'warn')">
              {{ microFastTailCleanupRows.length }} rows
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">depth5 optional</div><div class="plan-num">{{ microFastTailCleanupAudit.summary?.depth5_role_counts?.optional_evidence || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">cvd root none</div><div class="plan-num up">{{ microFastTailCleanupAudit.summary?.cvd_commit_trace_counts?.none || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">fast-z ok</div><div class="plan-num up">{{ microFastTailCleanupAudit.summary?.fast_z_nan_trace_counts?.ok || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">dwell judgeable</div><div class="plan-num">{{ microFastTailCleanupAudit.summary?.candidate_dwell_counts?.judgeable || 0 }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>Dwell</th><th>Age</th><th>CVD Root</th><th>CVD Sev</th><th>Fast-Z Split</th><th>Z Sev</th><th>Depth Role</th><th>Depth Class</th><th>Blocked</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microFastTailCleanupRows.slice(0, 200)" :key="`fast-tail-${row.run_id}-${row.symbol}`">
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.candidate_dwell?.dwell_state)">{{ row.candidate_dwell?.dwell_state || "-" }}</span></td>
                    <td>{{ Math.round(Number(row.candidate_dwell?.target_age_sec || 0)) }}s</td>
                    <td>{{ row.cvd_commit_missing_trace?.root_cause || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.cvd_commit_missing_trace?.severity)">{{ row.cvd_commit_missing_trace?.severity || "-" }}</span></td>
                    <td>{{ row.fast_z_nan_trace?.reason || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.fast_z_nan_trace?.severity)">{{ row.fast_z_nan_trace?.severity || "-" }}</span></td>
                    <td>{{ row.coverage_root_cause_v2?.partialDepth5?.role || "-" }}</td>
                    <td>{{ row.coverage_root_cause_v2?.partialDepth5?.coverage_class || "-" }}</td>
                    <td>{{ row.fast_z_nan_trace?.blocked_consumption ? "yes" : "no" }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microFastTailCleanupRows.length" class="notice"><span>No micro_fast tail cleanup rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Fast Judgeable Runtime</div>
            <span class="tag" :class="statusClass(microFastJudgeableRows.length ? 'ok' : 'warn')">
              {{ microFastJudgeableRows.length }} rows
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">not judgeable</div><div class="plan-num flat">{{ microFastJudgeableAudit.summary?.scope_counts?.not_judgeable_yet || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">z missing</div><div class="plan-num down">{{ microFastJudgeableAudit.summary?.scope_counts?.judgeable_but_z_missing || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">tech failed</div><div class="plan-num down">{{ microFastJudgeableAudit.summary?.scope_counts?.judgeable_and_technical_failed || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">ready</div><div class="plan-num up">{{ microFastJudgeableAudit.summary?.scope_counts?.judgeable_and_ready || 0 }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>Scope</th><th>Reason</th><th>Countable</th><th>Z Trace</th><th>NaN</th><th>Freshness</th><th>Stale Root</th><th>CVD</th><th>OFI</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microFastJudgeableRows.slice(0, 200)" :key="`fast-judge-${row.run_id}-${row.symbol}`">
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.judgeable_scope?.scope)">{{ row.judgeable_scope?.scope || "-" }}</span></td>
                    <td>{{ row.judgeable_scope?.reason || "-" }}</td>
                    <td>{{ row.judgeable_scope?.technical_failure_countable ? "yes" : "no" }}</td>
                    <td>{{ row.fast_z_append_read_trace?.trace_status || "-" }}</td>
                    <td>{{ row.fast_z_nan_trace?.reason || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.cvd_ofi_bucket_freshness_trace?.freshness_status)">{{ row.cvd_ofi_bucket_freshness_trace?.freshness_status || "-" }}</span></td>
                    <td>{{ row.cvd_ofi_bucket_freshness_trace?.stale_root_cause || "-" }}</td>
                    <td>{{ row.cvd_ofi_bucket_freshness_trace?.cvd_commit_state || "-" }}</td>
                    <td>{{ row.cvd_ofi_bucket_freshness_trace?.ofi_commit_state || "-" }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microFastJudgeableRows.length" class="notice"><span>No micro_fast judgeable rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Fast Judgeable-Only Metrics</div>
            <span class="tag" :class="statusClass(microFastJudgeableOnlyRows.length ? 'ok' : 'warn')">
              {{ microFastJudgeableOnlyRows.length }} rows
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">all rows</div><div class="plan-num">{{ microFastJudgeableOnlyAudit.summary?.all_rows || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">not judgeable</div><div class="plan-num flat">{{ microFastJudgeableOnlyAudit.summary?.not_judgeable_rows || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">judgeable</div><div class="plan-num up">{{ microFastJudgeableOnlyAudit.summary?.judgeable_rows || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">tech countable</div><div class="plan-num down">{{ microFastJudgeableOnlyAudit.summary?.technical_countable_rows || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">reader unknown</div><div class="plan-num down">{{ microFastJudgeableOnlyAudit.summary?.reader_window_short_root_cause_counts?.reader_window_short_unknown || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">invalid unknown</div><div class="plan-num down">{{ microFastJudgeableOnlyAudit.summary?.invalid_value_root_cause_counts?.fast_z_invalid_unknown || 0 }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>Scope</th><th>Reader Root</th><th>Avail/Req</th><th>Valid Ratio</th><th>Invalid Root</th><th>CVD Tail</th><th>Freshness</th><th>Raw</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microFastJudgeableOnlyRows.slice(0, 200)" :key="`fast-judge-only-${row.run_id}-${row.symbol}`">
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.judgeable_scope?.scope)">{{ row.judgeable_scope?.scope || "-" }}</span></td>
                    <td>{{ row.fast_z_reader_window_short_trace?.root_cause || "-" }}</td>
                    <td>{{ row.fast_z_reader_window_short_trace?.available_bucket_count ?? "-" }} / {{ row.fast_z_reader_window_short_trace?.required_bucket_count ?? "-" }}</td>
                    <td>{{ formatPct(row.fast_z_reader_window_short_trace?.valid_bucket_ratio) }}</td>
                    <td>{{ row.fast_z_invalid_value_trace?.root_cause || "-" }}</td>
                    <td>{{ row.cvd_commit_tail_trace?.root_cause || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.cvd_ofi_bucket_freshness_trace?.freshness_status)">{{ row.cvd_ofi_bucket_freshness_trace?.freshness_status || "-" }}</span></td>
                    <td>{{ (row.raw_reasons || []).join(", ") || "-" }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microFastJudgeableOnlyRows.length" class="notice"><span>No judgeable-only metrics for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Micro Fast Judgeable / Coverage / Valid Bucket</div>
            <span class="tag" :class="statusClass(microFastJudgeableThroughputRows.length ? 'ok' : 'warn')">
              {{ microFastJudgeableThroughputRows.length }} rows
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">judgeable yield</div><div class="plan-num up">{{ formatPct(microFastJudgeableThroughputAudit.summary?.judgeable_yield) }}</div></div>
              <div class="plan-box"><div class="plan-name">not judgeable</div><div class="plan-num flat">{{ microFastJudgeableThroughputAudit.summary?.not_judgeable_count || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">technical agg</div><div class="plan-num down">{{ microFastCoverageSplitAudit.summary?.coverage_group_counts?.aggTrade?.technical || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">technical book</div><div class="plan-num down">{{ microFastCoverageSplitAudit.summary?.coverage_group_counts?.bookTicker?.technical || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">valid low</div><div class="plan-num down">{{ microFastValidBucketAudit.summary?.low_valid_bucket_rows || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">observe pool</div><div class="plan-num">{{ microFastJudgeableThroughputAudit.summary?.observe_pool_counts?.observe_pool || 0 }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>Reason</th><th>Age</th><th>Dwell</th><th>Buckets</th><th>Valid</th><th>Source</th><th>Pool</th><th>Agg</th><th>Book</th><th>Valid Root</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microFastJudgeableThroughputRows.slice(0, 200)" :key="`fast-throughput-${row.run_id}-${row.symbol}`">
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.judgeable_throughput_trace?.not_judgeable_reason)">{{ row.judgeable_throughput_trace?.not_judgeable_reason || "-" }}</span></td>
                    <td>{{ Math.round(Number(row.judgeable_throughput_trace?.target_age_sec || 0)) }}s</td>
                    <td>{{ Math.round(Number(row.judgeable_throughput_trace?.dwell_sec || 0)) }}s / {{ row.judgeable_throughput_trace?.required_dwell_sec ?? "-" }}s</td>
                    <td>{{ row.judgeable_throughput_trace?.bucket_count ?? "-" }} / {{ row.judgeable_throughput_trace?.required_bucket_count ?? "-" }}</td>
                    <td>{{ formatPct(row.judgeable_throughput_trace?.valid_bucket_ratio) }}</td>
                    <td>{{ row.judgeable_throughput_trace?.target_source || "-" }}</td>
                    <td>{{ row.observe_pool_trace?.pool_state || "-" }}</td>
                    <td>{{ row.coverage_market_technical_split?.aggTrade?.group || "-" }}</td>
                    <td>{{ row.coverage_market_technical_split?.bookTicker?.group || "-" }}</td>
                    <td>{{ row.valid_bucket_ratio_low_trace?.root_cause || "-" }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microFastJudgeableThroughputRows.length" class="notice"><span>No judgeable throughput rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Full-Z Store Drilldown</div>
            <span class="tag" :class="statusClass(microFullZAudit.summary?.status_counts?.available ? 'ok' : 'warn')">
              {{ microFullZRows.length }} micro_full rows
            </span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">available</div><div class="plan-num up">{{ microFullZAudit.summary?.status_counts?.available || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">missing</div><div class="plan-num down">{{ microFullZAudit.summary?.status_counts?.missing || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">valid ratio low</div><div class="plan-num flat">{{ microFullZAudit.summary?.missing_reason_counts?.valid_bucket_ratio_low || 0 }}</div></div>
            </div>
            <div class="audit-symbol-table micro-quality-table">
              <table>
                <thead>
                  <tr><th>Symbol</th><th>State</th><th>Reason</th><th>Store</th><th>Rows Total</th><th>Loaded</th><th>Eligible</th><th>Rejected</th><th>Valid Ratio</th><th>Gap</th><th>Source</th><th>Reject Reasons</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in microFullZRows.slice(0, 200)" :key="`full-z-${row.run_id}-${row.symbol}`">
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.state)">{{ row.state || "-" }}</span></td>
                    <td>{{ row.full_z_missing_reason || row.attributed_reason || "-" }}</td>
                    <td><span class="tag" :class="statusClass(row.full_z_status)">{{ row.full_z_status || "-" }}</span></td>
                    <td>{{ row.store_window?.store_rows_total ?? "-" }}</td>
                    <td>{{ row.store_window?.reader_rows_loaded ?? "-" }}</td>
                    <td>{{ row.store_window?.eligible_rows ?? "-" }}</td>
                    <td>{{ row.store_window?.rejected_rows ?? "-" }}</td>
                    <td>{{ row.store_window?.valid_bucket_ratio ?? "-" }}</td>
                    <td>{{ row.store_window?.max_gap_sec ?? "-" }}</td>
                    <td>{{ row.target_source?.source_type || row.target_source?.tier || "-" }}</td>
                    <td class="path">{{ JSON.stringify(row.store_window?.reject_reason_counts || {}) }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!microFullZRows.length" class="notice"><span>No full-z drilldown rows for current run</span><span class="tag warn">empty</span></div>
            </div>
          </div>
        </div>

        <div class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Symbol Drilldown</div>
            <div class="tabs compact-tabs">
              <button v-for="tab in ['all', ...pipelineStrategyLineOrder]" :key="tab" :class="{ active: auditLineFilter === tab }" @click="auditLineFilter = tab">{{ tab === 'all' ? 'all' : lineLabel(tab) }}</button>
            </div>
          </div>
          <div class="panel-body audit-symbol-table">
            <table>
              <thead>
                <tr><th>Line</th><th>Symbol</th><th>Side</th><th>Action</th><th>Refresh</th><th>Micro</th><th>Exec</th><th>Reasons</th></tr>
              </thead>
              <tbody>
                <tr v-for="row in runAuditSymbols.slice(0, 300)" :key="`${row.strategy_line}-${row.symbol}`">
                  <td>{{ lineLabel(row.strategy_line) }}</td>
                  <td>{{ row.symbol }}</td>
                  <td>{{ row.decision }}</td>
                  <td>{{ row.action || row.entry_mode }}</td>
                  <td><span class="tag" :class="statusClass(row.refresh?.direction_still_valid === false ? 'failed' : row.refresh?.present ? 'pass' : 'missing')">{{ row.refresh?.present ? (row.refresh?.direction_still_valid === false ? 'invalid' : 'ok') : 'missing' }}</span></td>
                  <td>{{ row.micro_lifecycle?.state || row.micro_lifecycle?.status || (row.strategy_line === 'without_micro' ? 'n/a' : '-') }}</td>
                  <td><span class="tag" :class="row.executable ? 'good' : 'warn'">{{ row.executable ? 'yes' : 'no' }}</span></td>
                  <td class="path">{{ (row.reason_codes || []).slice(0, 4).join(", ") }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>

        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>P0 Failures</div><span class="tag bad">{{ runAudit.failure_count || 0 }}</span></div>
            <div class="panel-body audit-log-box">
              <div v-for="(row, idx) in runAudit.failures || []" :key="idx" class="log-line">{{ row.scope }} / {{ row.symbol || "-" }} / {{ row.name }} / {{ (row.reason_codes || []).join(", ") }}</div>
              <div v-if="!(runAudit.failures || []).length" class="notice"><span>No P0 failures</span><span class="tag good">pass</span></div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Warnings</div><span class="tag warn">{{ runAudit.warning_count || 0 }}</span></div>
            <div class="panel-body audit-log-box">
              <div v-for="(row, idx) in runAudit.warnings || []" :key="idx" class="log-line">{{ row.scope }} / {{ row.symbol || "-" }} / {{ row.name }} / {{ row.warning_class || "contract" }} / {{ row.detail || (row.reason_codes || []).join(", ") }}</div>
              <div v-if="!(runAudit.warnings || []).length" class="notice"><span>No warnings</span><span class="tag good">clean</span></div>
            </div>
          </article>
        </div>

        <div v-if="showAuditRawPayload" class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Raw Run Audit Payload</div><span class="tag warn">read only</span></div>
          <div class="panel-body"><pre>{{ JSON.stringify(runAudit || audit, null, 2) }}</pre></div>
        </div>
      </section>

      <section v-else-if="activePage === 'paper'">
        <PageHeader title="Paper Trading" subtitle="P14 市价-only 常驻模拟盘：三条策略线独立撮合、独立记账、独立统计；无 slot 或非市价信号直接 Pass。" />
        <div class="paper-toolbar">
          <div class="tabs">
            <button v-for="tab in ['overview', ...strategyLineOrder]" :key="tab" :class="{ active: paperTab === tab }" @click="paperTab = tab">{{ tab === 'overview' ? '总览' : lineLabel(tab) }}</button>
          </div>
          <div class="action-row">
            <button class="btn warn" :disabled="paperTab === 'overview' || Boolean(paperArchiveBusy)" @click="archiveResetPaperLine(paperTab)"><Archive />{{ paperArchiveBusy === paperTab ? "Archiving..." : "Archive & Reset" }}</button>
            <button class="btn" @click="refreshPaperRealtime"><RefreshCw />Refresh Paper</button>
            <span class="tag blue">auto 60s · last {{ paperLastRefreshAt || "-" }} · next {{ paperNextRefreshAt || "-" }}</span>
            <span class="tag blue">账本 All Ledger · 诊断 Current Run</span>
          </div>
        </div>
        <div v-if="paperArchiveMessage" class="notice"><span>{{ paperArchiveMessage }}</span><span class="tag blue">paper archive</span></div>
        <div class="grid-kpi">
          <article class="card kpi"><div class="kpi-label">Paper Daemon</div><div class="kpi-value status-value"><span v-if="isRunningHealth(paperHealth)" class="pulse-dot inline"></span>{{ paperHealth?.status || paperStatus?.status || "unknown" }}</div><div class="kpi-sub">1m matching · {{ healthText(paperHealth) }}</div></article>
          <article class="card kpi"><div class="kpi-label">Net PnL</div><div class="kpi-value" :class="pnlClass(selectedViewStats.net_pnl_usdt)">{{ money(selectedViewStats.net_pnl_usdt) }}</div><div class="kpi-sub">{{ selectedViewStats.scope }} · 含手续费/滑点</div></article>
          <article class="card kpi"><div class="kpi-label">Open Positions</div><div class="kpi-value">{{ openPositionRows.length }}</div><div class="kpi-sub">All Ledger · market entry only</div></article>
          <article class="card kpi"><div class="kpi-label">Current Run Pass</div><div class="kpi-value flat">{{ paperSkippedTotal }}</div><div class="kpi-sub">diagnostics total · sample {{ skippedSignalRows.length }}</div></article>
          <article class="card kpi"><div class="kpi-label">Avg Drift</div><div class="kpi-value flat">{{ money(paperRealismMetrics.avg_entry_drift_bps) }}</div><div class="kpi-sub">bps · entry reference drift</div></article>
          <article class="card kpi"><div class="kpi-label">Avg Slippage</div><div class="kpi-value flat">{{ money(paperRealismMetrics.avg_slippage_bps) }}</div><div class="kpi-sub">bps · fee {{ money(paperRealismMetrics.fee_usdt) }}U</div></article>
          <article class="card kpi"><div class="kpi-label">Fill Delay</div><div class="kpi-value flat">{{ money(paperRealismMetrics.avg_fill_delay_sec) }}</div><div class="kpi-sub">sec · executable to fill</div></article>
          <article class="card kpi"><div class="kpi-label">Reconciliation</div><div class="kpi-value">{{ paperReconciliationCounts.orders || 0 }} / {{ paperReconciliationCounts.fills || 0 }}</div><div class="kpi-sub">orders / fills · skips {{ paperReconciliationCounts.skips || 0 }}</div></article>
        </div>
        <div class="paper-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>持仓中 / {{ paperTab === 'overview' ? '总览' : lineLabel(paperTab) }}</div><span class="tag blue">All Ledger · opened_at desc</span></div>
            <div class="panel-body">
              <table><thead><tr><th>策略</th><th>交易对</th><th>方向</th><th>数量</th><th>开仓价</th><th>止损</th><th>止盈</th><th>浮盈亏</th></tr></thead><tbody>
                <tr v-for="row in openPositionRows" :key="row.id || row.symbol" @click="openPaperDetail(row.strategy_line || paperTab, row.symbol, row)">
                  <td>{{ lineLabel(row.strategy_line) }}</td><td>{{ row.symbol }}</td><td>{{ sideLabel(row.side) }}</td><td>{{ quantity(row.remaining_quantity || row.quantity) }}</td><td>{{ price(row.entry_price) }}</td><td>{{ price(row.stop_loss) }}</td><td>{{ price(row.take_profit) }}</td><td :class="pnlClass(row.unrealized_pnl_usdt)">{{ money(row.unrealized_pnl_usdt) }}</td>
                </tr>
                <tr v-if="!openPositionRows.length"><td colspan="8" class="empty-state">当前策略账本没有持仓。没有 executable market 信号时，paper 不会挂单。</td></tr>
              </tbody></table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>已结算 / {{ paperTab === 'overview' ? '总览' : lineLabel(paperTab) }}</div><span class="tag good">All Ledger · closed_at desc · {{ settledRows.length }} closed</span></div>
            <div class="panel-body">
              <table><thead><tr><th>策略</th><th>交易对</th><th>方向</th><th>开仓</th><th>平仓</th><th>盈亏</th><th>费用</th><th>原因</th></tr></thead><tbody>
                <tr v-for="row in settledRows" :key="row.id || row.order_id" @click="openPaperDetail(row.strategy_line || paperTab, row.symbol, row)">
                  <td>{{ lineLabel(row.strategy_line) }}</td><td>{{ row.symbol }}</td><td>{{ sideLabel(row.side) }}</td><td>{{ price(row.filled_entry_price || row.entry_price) }}</td><td>{{ price(row.exit_price) }}</td><td :class="pnlClass(row.realized_pnl_usdt)">{{ money(row.realized_pnl_usdt) }}</td><td>{{ money(row.fee_usdt) }}</td><td>{{ exitReasonLabel(row.exit_reason) }}</td>
                </tr>
                <tr v-if="!settledRows.length"><td colspan="8" class="empty-state">当前策略账本没有已结算订单。</td></tr>
              </tbody></table>
            </div>
          </article>
        </div>
        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>统计 / 策略全历史账本</div><span class="tag good">All Ledger</span></div>
            <div class="panel-body stat-cards">
              <div v-for="line in strategyLineOrder" :key="line" class="stat">
                <span>{{ lineLabel(line) }}</span>
                <strong :class="pnlClass(viewStatsForLine(line).net_pnl_usdt)">{{ money(viewStatsForLine(line).net_pnl_usdt) }}</strong>
                <small>全历史订单 {{ viewStatsForLine(line).total_orders || 0 }} · 已结算 {{ viewStatsForLine(line).closed_orders || 0 }} · 胜率 {{ money(viewStatsForLine(line).win_rate) }}% · AvgR {{ money(viewStatsForLine(line).avg_net_r) }} · WinR {{ money(viewStatsForLine(line).avg_win_r) }} · LossR {{ money(viewStatsForLine(line).avg_loss_r) }} · Risk {{ money(viewStatsForLine(line).avg_risk_usdt) }}U · 费用 {{ money(viewStatsForLine(line).fee_usdt) }}</small>
              </div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Current Run Diagnostics / 未开仓原因</div><span class="tag warn">sample {{ skippedSignalRows.length }} / total {{ paperSkippedTotal }}</span></div>
            <div class="panel-body">
              <table><thead><tr><th>策略</th><th>交易对</th><th>方向</th><th>动作</th><th>原因</th></tr></thead><tbody>
                <tr v-for="row in skippedSignalRows.slice(0, 16)" :key="`${row.strategy_line}-${row.symbol}-${row.source_plan_hash}`"><td>{{ lineLabel(row.strategy_line || row.line) }}</td><td>{{ row.symbol }}</td><td>{{ sideLabel(row.side) }}</td><td>{{ row.source_action || '-' }} / {{ row.source_entry_mode || '-' }}</td><td>{{ skipReasonLabel(row.skip_reason || row.reason) }}</td></tr>
                <tr v-if="!skippedSignalRows.length"><td colspan="5" class="empty-state">当前 run 没有 pass 记录。</td></tr>
              </tbody></table>
            </div>
          </article>
        </div>

        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Recent Realistic Fills</div><span class="tag blue">{{ paperRecentRealismFills.length }} fills</span></div>
          <div class="panel-body">
            <table><thead><tr><th>Line</th><th>Symbol</th><th>Action</th><th>Planned</th><th>Reference</th><th>Fill</th><th>Drift</th><th>Slip</th><th>Fee</th><th>Delay</th></tr></thead><tbody>
              <tr v-for="row in paperRecentRealismFills.slice(0, 20)" :key="row.id">
                <td>{{ lineLabel(row.strategy_line) }}</td><td>{{ row.symbol }}</td><td>{{ row.action }}</td><td>{{ price(row.planned_entry_price) }}</td><td>{{ price(row.reference_price) }}</td><td>{{ price(row.fill_price) }}</td><td>{{ money(row.entry_drift_bps) }} bps</td><td>{{ money(row.slippage_bps) }} bps</td><td>{{ money(row.fee_usdt) }}U</td><td>{{ money(row.fill_delay_sec) }}s</td>
              </tr>
              <tr v-if="!paperRecentRealismFills.length"><td colspan="10" class="empty-state">No fills for the selected paper scope.</td></tr>
            </tbody></table>
          </div>
        </article>
      </section>

      <section v-else-if="activePage === 'trade-quality'">
        <PageHeader title="Trade Quality" subtitle="P19 diagnostic samples: source packages, 1m replay, R-first MFE/MAE, and non-mutating root-cause analysis." />
        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Trade Package Selector</div>
            <div class="title-actions">
              <span class="tag" :class="tradeQualitySyncStatus.stale ? 'warn' : 'good'">{{ tradeQualitySyncStatus.stale ? "sync stale" : "sync cached" }}</span>
              <span class="tag" :class="tradeQualityLoading ? 'warn' : 'good'">{{ tradeQualityLoading ? "loading" : "ready" }}</span>
              <span class="tag" :class="tradeQualityDetailsLoading ? 'warn' : 'blue'">{{ tradeQualityDetailsLoading ? "details loading" : "details cached" }}</span>
            </div>
          </div>
          <div class="panel-body form-grid">
            <label>source
              <select v-model="tradeQualityFilters.source">
                <option value="all">all migrated history</option>
                <option value="current_paper">current paper closed</option>
                <option value="archive">archive records</option>
                <option value="backtest_p21_v2">backtest p21 v2</option>
                <option value="legacy_p18">legacy P18 live</option>
              </select>
            </label>
            <label v-if="tradeQualityFilters.source === 'archive'">archive package
              <select v-model="tradeQualityFilters.archive_id">
                <option value="">select archive package</option>
                <option v-for="pkg in tradeQualityPackageRows" :key="pkg.archive_id" :value="pkg.archive_id">
                  {{ pkg.archive_id }} · {{ pkg.strategy_line }} · {{ pkg.diagnostic_sample_count || 0 }} samples · {{ formatPct(pkg.replay_coverage) }}
                </option>
              </select>
            </label>
            <label v-if="tradeQualityFilters.source === 'backtest_p21_v2'">backtest package
              <select v-model="tradeQualityFilters.package_key" @change="selectTradeQualityBacktestPackageFromFilter">
                <option value="">select backtest package</option>
                <option v-for="pkg in tradeQualityPackageRows" :key="pkg.package_key" :value="pkg.package_key">
                  {{ tradeQualityBacktestPackageLabel(pkg) }}
                </option>
              </select>
            </label>
            <label v-if="tradeQualityFilters.source !== 'backtest_p21_v2'">archive id<input v-model="tradeQualityFilters.archive_id" placeholder="paper_exp_..." /></label>
            <label>strategy
              <select v-model="tradeQualityFilters.strategy_line">
                <option value="all">all</option>
                <option value="without_micro">without_micro</option>
                <option value="micro_fast">micro_fast</option>
                <option value="micro_full">micro_full</option>
                <option value="strategy4">strategy4</option>
                <option value="strategy5">strategy5</option>
                <option value="strategy6">strategy6</option>
              </select>
            </label>
            <label>side
              <select v-model="tradeQualityFilters.side">
                <option value="all">all</option>
                <option value="LONG">LONG</option>
                <option value="SHORT">SHORT</option>
              </select>
            </label>
            <label>exit
              <select v-model="tradeQualityFilters.exit_reason">
                <option value="all">all</option>
                <option value="SL">SL</option>
                <option value="TP">TP</option>
                <option value="time_stop">time_stop</option>
                <option value="manual">manual</option>
                <option value="signal_reverse">signal_reverse</option>
              </select>
            </label>
            <label>root cause
              <select v-model="tradeQualityFilters.root_cause">
                <option value="all">all</option>
                <option v-for="row in tradeQualityRootCauses" :key="row.key" :value="row.key">{{ row.key }}</option>
              </select>
            </label>
            <label>entry quality
              <select v-model="tradeQualityFilters.entry_quality_label">
                <option value="all">all</option>
                <option v-for="row in tradeQualityEntryQualityRows" :key="row.label" :value="row.label">{{ row.label }}</option>
              </select>
            </label>
            <label>microstructure
              <select v-model="tradeQualityFilters.entry_quality_v2_label">
                <option value="all">all</option>
                <option v-for="row in tradeQualityEntryMicroRows" :key="row.label" :value="row.label">{{ row.label }}</option>
              </select>
            </label>
            <label>market context
              <select v-model="tradeQualityFilters.market_context_label">
                <option value="all">all</option>
                <option v-for="row in tradeQualityEntryMarketRows" :key="row.label" :value="row.label">{{ row.label }}</option>
              </select>
            </label>
            <label>entry context v3
              <select v-model="tradeQualityFilters.entry_context_v3_label">
                <option value="all">all</option>
                <option v-for="row in tradeQualityEntryV3Rows" :key="row.label" :value="row.label">{{ row.label }}</option>
              </select>
            </label>
            <label>micro evidence
              <select v-model="tradeQualityFilters.microstructure_coverage">
                <option value="all">all</option>
                <option value="complete">complete</option>
                <option value="partial_micro_missing">partial_micro_missing</option>
                <option value="missing_micro_evidence">missing_micro_evidence</option>
                <option value="micro_evidence_not_required">micro_evidence_not_required</option>
              </select>
            </label>
            <label>market evidence
              <select v-model="tradeQualityFilters.market_context_status">
                <option value="all">all</option>
                <option value="complete">complete</option>
                <option value="partial_oi_missing">partial_oi_missing</option>
                <option value="partial_funding_missing">partial_funding_missing</option>
                <option value="partial_btc_missing">partial_btc_missing</option>
                <option value="missing_market_context">missing_market_context</option>
              </select>
            </label>
            <label>funding
              <select v-model="tradeQualityFilters.funding_regime">
                <option value="all">all</option>
                <option value="OVERHEATED">OVERHEATED</option>
                <option value="NEGATIVE_EXTREME">NEGATIVE_EXTREME</option>
                <option value="WARM">WARM</option>
                <option value="NEUTRAL">NEUTRAL</option>
                <option value="missing">missing</option>
              </select>
            </label>
            <label>OI direction
              <select v-model="tradeQualityFilters.oi_direction">
                <option value="all">all</option>
                <option value="up">up</option>
                <option value="down">down</option>
                <option value="flat">flat</option>
                <option value="missing">missing</option>
              </select>
            </label>
            <label>BTC alignment
              <select v-model="tradeQualityFilters.btc_alignment">
                <option value="all">all</option>
                <option value="same">same</option>
                <option value="opposite">opposite</option>
                <option value="neutral">neutral</option>
                <option value="missing_btc_candle">missing_btc_candle</option>
              </select>
            </label>
            <label>replay
              <select v-model="tradeQualityFilters.replay_status">
                <option value="all">all</option>
                <option value="candle_1m_replay">candle_1m_replay</option>
                <option value="proxy_or_missing">proxy_or_missing</option>
                <option value="missing_1m_replay">missing_1m_replay</option>
              </select>
            </label>
            <label>quality tag<input v-model="tradeQualityFilters.quality_tag" placeholder="mfe_lt_0.3" /></label>
            <label>symbol<input v-model="tradeQualityFilters.symbol" placeholder="BTCUSDT" /></label>
            <label>rows<input v-model.number="tradeQualityFilters.limit" type="number" min="20" max="500" /></label>
            <div class="action-row">
              <button class="btn primary" :disabled="tradeQualityRefreshBusy || tradeQualityLoading || tradeQualityDetailsLoading" @click="runTradeQualityRefreshEnrich">
                <RefreshCw />{{ tradeQualityRefreshBusy ? "Refreshing..." : (tradeQualityFilters.source === 'backtest_p21_v2' ? "Refresh Backtest" : "Refresh & Enrich") }}
              </button>
              <button v-if="tradeQualityFilters.source === 'backtest_p21_v2'" class="btn" :disabled="tradeQualityRefreshBusy || tradeQualityBacktestPackageBlocked(selectedTradeQualityPackage)" @click="runTradeQualityBacktestMaterialize(true)">
                <RefreshCw />Dry Run
              </button>
              <button v-if="tradeQualityFilters.source === 'backtest_p21_v2'" class="btn cycle" :disabled="tradeQualityRefreshBusy || tradeQualityBacktestPackageBlocked(selectedTradeQualityPackage)" @click="runTradeQualityBacktestMaterialize(false)">
                <DatabaseZap />Materialize Bounded
              </button>
              <span class="tag" :class="tradeQualityRefreshBusy ? 'warn' : 'blue'">{{ tradeQualityRefreshStage || "idle" }}</span>
              <span v-if="tradeQualityRefreshLast?.status" class="tag" :class="tradeQualityRefreshLast.status === 'ok' ? 'good' : 'warn'">{{ tradeQualityRefreshLast.status }}</span>
            </div>
          </div>
          <div v-if="tradeQualityLazyMessage" class="panel-body">
            <div class="notice"><span>{{ tradeQualityLazyMessage }}</span></div>
          </div>
          <div v-if="tradeQualityFilters.source === 'current_paper' && tradeQualitySummary.paper_epoch_scope" class="panel-body">
            <div class="notice">
              <span>current paper is scoped to latest paper archive epochs per strategy line.</span>
              <span class="tag blue">{{ tradeQualitySummary.active_strategy_epochs?.length || 0 }} active scopes</span>
              <span v-if="tradeQualitySummary.excluded_stale_current_paper_samples" class="tag warn">
                {{ tradeQualitySummary.excluded_stale_current_paper_samples }} stale samples excluded
              </span>
              <span v-else class="tag good">no stale current paper cache</span>
            </div>
          </div>
          <div class="panel-body package-meta-grid">
            <div class="stat">
              <span>sync status</span>
              <strong>{{ tradeQualityFilters.source === 'backtest_p21_v2' ? "backtest" : (tradeQualitySyncStatus.stale ? "stale" : "cached") }}</strong>
              <small>{{ tradeQualityFilters.source === 'backtest_p21_v2' ? "leaderboard candidates" : (tradeQualitySyncStatus.last_synced_at || "-") }}</small>
            </div>
            <div class="stat">
              <span>packages</span>
              <strong>{{ tradeQualityPackageRows.length }}</strong>
              <small>{{ tradeQualityFilters.source === 'backtest_p21_v2' ? (tradeQualityFilters.strategy_line === 'all' ? "global top 30" : "strategy top 10") : (tradeQualitySyncStatus.next_recommended_action || "read_cache") }}</small>
            </div>
            <div class="stat">
              <span>selected package</span>
              <strong>{{ tradeQualityFilters.source === 'current_paper' ? (tradeQualitySummary.sample_count || 0) : (selectedTradeQualityPackage?.diagnostic_sample_count || selectedTradeQualityPackage?.materialized_sample_count || 0) }}</strong>
              <small>{{ tradeQualityFilters.source === 'current_paper' ? "active paper epoch scope" : (tradeQualityFilters.source === 'backtest_p21_v2' ? (selectedTradeQualityPackage?.package_key || "select backtest package") : (selectedTradeQualityPackage?.archive_id || "all / manual archive id")) }}</small>
            </div>
            <div class="stat">
              <span>{{ tradeQualityFilters.source === 'current_paper' ? "epoch boundary" : (tradeQualityFilters.source === 'backtest_p21_v2' ? "materialize status" : "package replay") }}</span>
              <strong>{{ tradeQualityFilters.source === 'current_paper' ? (tradeQualitySummary.excluded_stale_current_paper_samples || 0) : (tradeQualityFilters.source === 'backtest_p21_v2' ? (selectedTradeQualityPackage?.sample_status || "unselected") : formatPct(selectedTradeQualityPackage?.replay_coverage)) }}</strong>
              <small>{{ tradeQualityFilters.source === 'current_paper' ? "stale current_paper samples excluded" : (tradeQualityFilters.source === 'backtest_p21_v2' ? `${selectedTradeQualityPackage?.shadow_order_count || 0} shadow orders · bounded materialize only` : (selectedTradeQualityPackage ? `${selectedTradeQualityPackage.replay_count || 0} replay / ${selectedTradeQualityPackage.proxy_or_missing_count || 0} backlog` : "select archive package")) }}</small>
            </div>
          </div>
        </article>

        <div class="grid-kpi">
          <article class="card kpi"><div class="kpi-label">Trades</div><div class="kpi-value">{{ tradeQualityPerformanceStats.trade_count ?? tradeQualitySummary.sample_count ?? 0 }}</div><div class="kpi-sub">diagnostic samples</div></article>
          <article class="card kpi"><div class="kpi-label">Win Rate</div><div class="kpi-value flat">{{ formatPct(tradeQualityPerformanceStats.win_rate ?? tradeQualitySummary.win_rate) }}</div><div class="kpi-sub">{{ tradeQualityPerformanceStats.win_count || 0 }} win / {{ tradeQualityPerformanceStats.loss_count || 0 }} loss</div></article>
          <article class="card kpi"><div class="kpi-label">Expectancy</div><div class="kpi-value" :class="pnlClass(tradeQualityPerformanceStats.expectancy_R)">{{ signedR(tradeQualityPerformanceStats.expectancy_R) }}</div><div class="kpi-sub">win × avg win - loss × avg loss</div></article>
          <article class="card kpi"><div class="kpi-label">Profit / Loss</div><div class="kpi-value flat">{{ ratioX(tradeQualityPerformanceStats.profit_loss_ratio) }}</div><div class="kpi-sub">avg win / avg loss</div></article>
          <article class="card kpi"><div class="kpi-label">Max Drawdown</div><div class="kpi-value down">{{ signedR(-(tradeQualityPerformanceStats.max_drawdown_R || 0)) }}</div><div class="kpi-sub">cumulative net R path</div></article>
          <article class="card kpi"><div class="kpi-label">Avg Win R</div><div class="kpi-value up">{{ signedR(tradeQualityPerformanceStats.avg_win_R) }}</div><div class="kpi-sub">winning trades only</div></article>
          <article class="card kpi"><div class="kpi-label">Avg Loss R</div><div class="kpi-value down">{{ signedR(-(tradeQualityPerformanceStats.avg_loss_R || 0)) }}</div><div class="kpi-sub">loss magnitude</div></article>
          <article class="card kpi"><div class="kpi-label">R Parity</div><div class="kpi-value" :class="pnlClass(-(tradeQualityPerformanceStats.r_parity?.loss_overrun_ratio || 0))">{{ formatPct(tradeQualityPerformanceStats.r_parity?.loss_overrun_ratio) }}</div><div class="kpi-sub">loss overrun &gt; 1R</div></article>
          <article class="card kpi"><div class="kpi-label">Win-Loss Delta</div><div class="kpi-value" :class="pnlClass(tradeQualityPerformanceStats.r_parity?.avg_win_minus_loss_R)">{{ signedR(tradeQualityPerformanceStats.r_parity?.avg_win_minus_loss_R) }}</div><div class="kpi-sub">{{ tradeQualityPerformanceStats.r_parity?.diagnosis || "R parity evidence" }}</div></article>
          <article class="card kpi"><div class="kpi-label">Loss Streak</div><div class="kpi-value flat">{{ tradeQualityPerformanceStats.max_losing_streak || 0 }}</div><div class="kpi-sub">{{ losingStreakLabel(tradeQualityPerformanceStats) }}</div></article>
          <article class="card kpi"><div class="kpi-label">Fee / Profit</div><div class="kpi-value flat">{{ formatPct(tradeQualityPerformanceStats.fee_to_gross_profit_ratio) }}</div><div class="kpi-sub">fee {{ money(tradeQualityPerformanceStats.fee_total) }} / profit {{ money(tradeQualityPerformanceStats.gross_profit_usdt) }}</div></article>
          <article class="card kpi"><div class="kpi-label">Avg Hold</div><div class="kpi-value flat">{{ minutesLabel(tradeQualityPerformanceStats.avg_holding_minutes) }}</div><div class="kpi-sub">mean holding time</div></article>
          <article class="card kpi"><div class="kpi-label">Median Hold</div><div class="kpi-value flat">{{ minutesLabel(tradeQualityPerformanceStats.median_holding_minutes) }}</div><div class="kpi-sub">distribution center</div></article>
          <article class="card kpi"><div class="kpi-label">1m Replay</div><div class="kpi-value flat">{{ formatPct(tradeQualitySummary.replay_coverage) }}</div><div class="kpi-sub">MFE {{ signedR(tradeQualitySummary.avg_MFE_R) }} / MAE {{ signedR(tradeQualitySummary.avg_MAE_R) }}</div></article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Phenomenon Distribution</div>
            <span class="tag warn">diagnostic only</span>
          </div>
          <div class="panel-body">
            <div class="notice">
              <span>MFE / MAE explains whether losses are direction errors, early entries, stop/take-profit problems, or weak momentum.</span>
              <span class="tag blue">{{ tradeQualitySummary.phenomenon_sample_count || 0 }} replay samples</span>
              <span v-if="tradeQualitySummary.phenomenon_replay_required" class="tag warn">partial replay coverage</span>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Phenomenon</th><th>Meaning</th><th>Count</th><th>Ratio</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityPhenomena" :key="row.code">
                    <td><span class="tag" :class="statusClass(row.code)">{{ row.phenomenon }}</span></td>
                    <td>{{ row.meaning }}</td>
                    <td>{{ row.count }}</td>
                    <td>{{ formatPct(row.ratio) }}</td>
                  </tr>
                  <tr v-if="!tradeQualityPhenomena.length"><td colspan="4" class="empty-state">No phenomenon distribution yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Root Cause Attribution</div>
            <span class="tag warn">diagnostic only</span>
          </div>
          <div class="panel-body">
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">Top Loss Cause</div><div class="kpi-value flat">{{ tradeQualityRootCauseAttribution.top_loss_root_cause || "-" }}</div><div class="kpi-sub">largest loss contributor</div></article>
              <article class="card kpi"><div class="kpi-label">Loss Samples</div><div class="kpi-value down">{{ tradeQualityRootCauseAttribution.loss_sample_count || 0 }}</div><div class="kpi-sub">negative net R trades</div></article>
              <article class="card kpi"><div class="kpi-label">Coverage</div><div class="kpi-value flat">{{ formatPct(tradeQualityRootCauseAttribution.coverage) }}</div><div class="kpi-sub">{{ tradeQualityRootCauseAttribution.sample_count || 0 }} attributed samples</div></article>
              <article class="card kpi"><div class="kpi-label">Needs Replay</div><div class="kpi-value warn">{{ tradeQualityRootCauseAttribution.needs_replay_count || 0 }}</div><div class="kpi-sub">missing MFE / MAE path</div></article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Root Cause</th><th>Meaning</th><th>Count</th><th>Loss</th><th>Ratio</th><th>Avg R</th><th>MFE</th><th>MAE</th><th>Optimization Direction</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityRootCauseAttributionItems" :key="row.root_cause" @click="selectTradeQualityRootCause(row.root_cause)">
                    <td><span class="tag" :class="statusClass(row.root_cause)">{{ row.root_cause }}</span></td>
                    <td>{{ row.meaning }}</td>
                    <td>{{ row.count }}</td>
                    <td>{{ row.loss_count }}</td>
                    <td>{{ formatPct(row.ratio) }}</td>
                    <td :class="pnlClass(row.avg_net_R)">{{ signedR(row.avg_net_R) }}</td>
                    <td>{{ signedR(row.avg_MFE_R) }}</td>
                    <td>{{ signedR(row.avg_MAE_R) }}</td>
                    <td class="reason-cell">{{ row.optimization }}</td>
                  </tr>
                  <tr v-if="!tradeQualityRootCauseAttributionItems.length"><td colspan="9" class="empty-state">No root-cause attribution yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Deep Evidence V4</div>
            <div class="title-actions">
              <span class="tag warn">baseline shadow-only</span>
              <button class="btn tiny" :disabled="tradeQualityV4Loading" @click="runTradeQualityV4Materialize"><RefreshCw />Materialize V4</button>
              <button class="btn tiny cycle" :disabled="tradeQualityV4Loading" @click="runTradeQualityV4GateCandidates"><DatabaseZap />Generate Gates</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="notice subtle">
              <span>V4 adds entry-known RSI, Bollinger, EMA/VWAP distance, ATR, volume, CVD proxy, spread/depth proxy, and deep subcause labels.</span>
              <span class="tag blue">{{ tradeQualityV4Message || tradeQualityV4Summary.schema_version || "read-only" }}</span>
            </div>
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">V4 Features</div><div class="kpi-value flat">{{ tradeQualityV4Summary.feature_count || 0 }}</div><div class="kpi-sub">entry-known evidence rows</div></article>
              <article class="card kpi"><div class="kpi-label">Deep Roots</div><div class="kpi-value flat">{{ tradeQualityV4Summary.deep_root_count || 0 }}</div><div class="kpi-sub">subcause attribution rows</div></article>
              <article class="card kpi"><div class="kpi-label">Shadow Gates</div><div class="kpi-value warn">{{ tradeQualityV4Summary.gate_candidate_count || 0 }}</div><div class="kpi-sub">not applied to config</div></article>
              <article class="card kpi"><div class="kpi-label">No Lookahead</div><div class="kpi-value flat">pass</div><div class="kpi-sub">targets stay diagnostic only</div></article>
            </div>
            <div class="wide-grid">
              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Feature Coverage</div><span class="tag blue">{{ tradeQualityV4CoverageRows.length }} rows</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Strategy</th><th>Completeness</th><th>Proxy</th><th>Count</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityV4CoverageRows" :key="`${row.strategy_line}-${row.feature_completeness}-${row.proxy_level}`">
                        <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                        <td>{{ row.feature_completeness }}</td>
                        <td>{{ row.proxy_level }}</td>
                        <td>{{ row.count }}</td>
                      </tr>
                      <tr v-if="!tradeQualityV4CoverageRows.length"><td colspan="4" class="empty-state">No V4 feature coverage yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>
              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Deep Subcause</div><span class="tag blue">{{ tradeQualityV4DeepRows.length }} rows</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Strategy</th><th>Root</th><th>Subcause</th><th>Family</th><th>Samples</th><th>Confidence</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityV4DeepRows.slice(0, 30)" :key="`${row.strategy_line}-${row.root_cause}-${row.deep_subcause}`">
                        <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                        <td><span class="tag" :class="statusClass(row.root_cause)">{{ row.root_cause }}</span></td>
                        <td>{{ row.deep_subcause }}</td>
                        <td>{{ row.subcause_family }}</td>
                        <td>{{ row.sample_count }}</td>
                        <td>{{ formatNumber(row.avg_confidence, 2) }}</td>
                      </tr>
                      <tr v-if="!tradeQualityV4DeepRows.length"><td colspan="6" class="empty-state">No V4 deep root cause rows yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Strategy</th><th>Gate</th><th>Rule</th><th>Before</th><th>After Probe</th><th>Leakage</th><th>Risk</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityV4GateRows.slice(0, 30)" :key="row.candidate_id">
                    <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                    <td>{{ row.gate_type }}</td>
                    <td class="reason-cell">{{ shortJson(row.rule || {}, 220) }}</td>
                    <td class="reason-cell">{{ shortJson(row.metrics_before || {}, 220) }}</td>
                    <td class="reason-cell">{{ shortJson(row.metrics_after || {}, 220) }}</td>
                    <td>{{ row.leakage_check_status }}</td>
                    <td><span class="tag warn">{{ row.overfit_risk }}</span></td>
                  </tr>
                  <tr v-if="!tradeQualityV4GateRows.length"><td colspan="7" class="empty-state">No V4 shadow gate candidates yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Deep Evidence V5</div>
            <div class="title-actions">
              <span class="tag warn">shadow-only · baseline</span>
              <button class="btn tiny" :disabled="tradeQualityV5Loading" @click="runTradeQualityV5Materialize"><RefreshCw />Materialize V5</button>
              <button class="btn tiny cycle" :disabled="tradeQualityV5Loading" @click="runTradeQualityV5GateCandidates"><DatabaseZap />Generate V5 Gates</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="notice subtle">
              <span>V5 splits root causes into direction, entry timing, TP realism, profit pattern, market regime, and liquidity/cost factors. Gate rules use entry-known fields only.</span>
              <span class="tag blue">{{ tradeQualityV5Message || tradeQualityV5Summary.schema_version || "read-only" }}</span>
            </div>
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">V5 Causal Rows</div><div class="kpi-value flat">{{ tradeQualityV5Summary.causal_count || 0 }}</div><div class="kpi-sub">deep factor samples</div></article>
              <article class="card kpi"><div class="kpi-label">V5 Gates</div><div class="kpi-value warn">{{ tradeQualityV5Summary.gate_count || 0 }}</div><div class="kpi-sub">shadow candidates</div></article>
              <article class="card kpi"><div class="kpi-label">Rule Fields</div><div class="kpi-value flat">{{ (tradeQualityV5Summary.entry_known_rule_fields || []).length }}</div><div class="kpi-sub">entry-known only</div></article>
              <article class="card kpi"><div class="kpi-label">Writer Coverage</div><div class="kpi-value flat">{{ tradeQualityV5CoverageRows.length }}</div><div class="kpi-sub">source quality buckets</div></article>
            </div>
            <div class="wide-grid">
              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>V5 Factor Rollup</div><span class="tag blue">{{ tradeQualityV5RollupRows.length }} rows</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Strategy</th><th>Root</th><th>Direction</th><th>Entry</th><th>TP</th><th>Profit</th><th>Regime</th><th>Cost</th><th>Rows</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityV5RollupRows.slice(0, 40)" :key="`${row.strategy_line}-${row.root_cause}-${row.direction_factor_v5}-${row.entry_timing_factor_v5}-${row.tp_realism_factor_v5}`">
                        <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                        <td><span class="tag" :class="statusClass(row.root_cause)">{{ row.root_cause }}</span></td>
                        <td>{{ row.direction_factor_v5 }}</td>
                        <td>{{ row.entry_timing_factor_v5 }}</td>
                        <td>{{ row.tp_realism_factor_v5 }}</td>
                        <td>{{ row.profit_factor_v5 }}</td>
                        <td>{{ row.market_regime_factor_v5 }}</td>
                        <td>{{ row.liquidity_cost_factor_v5 }}</td>
                        <td>{{ row.rows }}</td>
                      </tr>
                      <tr v-if="!tradeQualityV5RollupRows.length"><td colspan="9" class="empty-state">No V5 causal rollup yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>
              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>V5 Writer Coverage</div><span class="tag blue">{{ tradeQualityV5CoverageRows.length }} buckets</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Strategy</th><th>P24 Match</th><th>Proxy</th><th>Rows</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityV5CoverageRows.slice(0, 30)" :key="`${row.strategy_line}-${row.p24_match}-${row.proxy_level}`">
                        <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                        <td>{{ row.p24_match || "unknown" }}</td>
                        <td>{{ row.proxy_level || "unknown" }}</td>
                        <td>{{ row.rows }}</td>
                      </tr>
                      <tr v-if="!tradeQualityV5CoverageRows.length"><td colspan="4" class="empty-state">No V5 writer coverage yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Strategy</th><th>Rule</th><th>Before PF</th><th>After PF</th><th>Test PF</th><th>Removed</th><th>Risk</th><th>Recommendation</th><th>Patch</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityV5GateRows.slice(0, 40)" :key="row.validation_id">
                    <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                    <td class="reason-cell">{{ row.rule?.field }}={{ row.rule?.value }}</td>
                    <td>{{ formatNumber(row.aggregate_metrics?.before?.pf, 3) }}</td>
                    <td>{{ formatNumber(row.aggregate_metrics?.after?.pf, 3) }}</td>
                    <td>{{ formatNumber(row.split_metrics?.test?.after?.pf, 3) }}</td>
                    <td>{{ formatPct(row.aggregate_metrics?.removed_coverage) }}</td>
                    <td><span class="tag warn">{{ row.overfit_risk }}</span></td>
                    <td><span class="tag blue">{{ row.recommendation }}</span></td>
                    <td class="reason-cell">{{ shortJson(row.config_patch_preview || {}, 220) }}</td>
                  </tr>
                  <tr v-if="!tradeQualityV5GateRows.length"><td colspan="9" class="empty-state">No V5 shadow gate candidates yet.</td></tr>
                </tbody>
              </table>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Strategy</th><th>Symbol</th><th>Root</th><th>Direction</th><th>Entry</th><th>TP</th><th>Profit</th><th>Confidence</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityV5CausalRows.slice(0, 40)" :key="row.causal_id">
                    <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                    <td>{{ row.symbol }}</td>
                    <td><span class="tag" :class="statusClass(row.root_cause)">{{ row.root_cause }}</span></td>
                    <td>{{ row.direction_factor_v5 }}</td>
                    <td>{{ row.entry_timing_factor_v5 }}</td>
                    <td>{{ row.tp_realism_factor_v5 }}</td>
                    <td>{{ row.profit_factor_v5 }}</td>
                    <td>{{ formatNumber(row.confidence_v5, 2) }}</td>
                  </tr>
                  <tr v-if="!tradeQualityV5CausalRows.length"><td colspan="8" class="empty-state">No V5 causal rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Dimension Attribution</div>
            <span class="tag warn">diagnostic only</span>
          </div>
          <div class="panel-body">
            <p class="muted small">Second-layer attribution by symbol, UTC hour, holding bucket, and side. Market context is reserved until per-trade regime evidence is available.</p>
            <div class="wide-grid">
              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Symbol</div><span class="tag blue">{{ tradeQualityDimensionSymbolRows.length }} rows</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Symbol</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Fee</th><th>Top Cause</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityDimensionSymbolRows" :key="row.key" @click="selectTradeQualityDimension(row)">
                        <td><span class="tag blue">{{ row.key }}</span></td>
                        <td>{{ row.trade_count }}</td>
                        <td>{{ row.loss_count }}</td>
                        <td>{{ formatPct(row.win_rate) }}</td>
                        <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                        <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                        <td>{{ signedR(row.avg_MFE_R) }}</td>
                        <td>{{ signedR(row.avg_MAE_R) }}</td>
                        <td>{{ ratioX(row.fee_ratio) }}</td>
                        <td><span class="tag" :class="statusClass(row.top_root_cause)" @click.stop="selectTradeQualityRootCause(row.top_root_cause)">{{ row.top_root_cause }}</span></td>
                      </tr>
                      <tr v-if="!tradeQualityDimensionSymbolRows.length"><td colspan="10" class="empty-state">No symbol attribution yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>

              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>UTC Hour</div><span class="tag blue">{{ tradeQualityDimensionHourRows.length }} rows</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Hour</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>Top Cause</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityDimensionHourRows" :key="row.key">
                        <td><span class="tag">{{ row.key }}</span></td>
                        <td>{{ row.trade_count }}</td>
                        <td>{{ row.loss_count }}</td>
                        <td>{{ formatPct(row.win_rate) }}</td>
                        <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                        <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                        <td><span class="tag" :class="statusClass(row.top_root_cause)" @click.stop="selectTradeQualityRootCause(row.top_root_cause)">{{ row.top_root_cause }}</span></td>
                      </tr>
                      <tr v-if="!tradeQualityDimensionHourRows.length"><td colspan="7" class="empty-state">No hour attribution yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>
            </div>

            <div class="wide-grid">
              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Holding Bucket</div><span class="tag blue">{{ tradeQualityDimensionHoldingRows.length }} rows</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Bucket</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Top Cause</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityDimensionHoldingRows" :key="row.key">
                        <td><span class="tag">{{ row.key }}</span></td>
                        <td>{{ row.trade_count }}</td>
                        <td>{{ row.loss_count }}</td>
                        <td>{{ formatPct(row.win_rate) }}</td>
                        <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                        <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                        <td>{{ signedR(row.avg_MFE_R) }}</td>
                        <td>{{ signedR(row.avg_MAE_R) }}</td>
                        <td><span class="tag" :class="statusClass(row.top_root_cause)" @click.stop="selectTradeQualityRootCause(row.top_root_cause)">{{ row.top_root_cause }}</span></td>
                      </tr>
                      <tr v-if="!tradeQualityDimensionHoldingRows.length"><td colspan="9" class="empty-state">No holding attribution yet.</td></tr>
                    </tbody>
                  </table>
                </div>
              </article>

              <article class="card panel">
                <div class="panel-header"><div class="panel-title"><span class="accent"></span>Side / Market Context</div><span class="tag warn">{{ tradeQualityDimensionMarketContext.status || "pending" }}</span></div>
                <div class="panel-body table-scroll">
                  <table>
                    <thead><tr><th>Side</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Top Cause</th></tr></thead>
                    <tbody>
                      <tr v-for="row in tradeQualityDimensionSideRows" :key="row.key" @click="selectTradeQualityDimension(row)">
                        <td><span class="tag" :class="statusClass(row.key)">{{ row.key }}</span></td>
                        <td>{{ row.trade_count }}</td>
                        <td>{{ row.loss_count }}</td>
                        <td>{{ formatPct(row.win_rate) }}</td>
                        <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                        <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                        <td>{{ signedR(row.avg_MFE_R) }}</td>
                        <td>{{ signedR(row.avg_MAE_R) }}</td>
                        <td><span class="tag" :class="statusClass(row.top_root_cause)" @click.stop="selectTradeQualityRootCause(row.top_root_cause)">{{ row.top_root_cause }}</span></td>
                      </tr>
                      <tr v-if="!tradeQualityDimensionSideRows.length"><td colspan="9" class="empty-state">No side attribution yet.</td></tr>
                    </tbody>
                  </table>
                  <div class="inline-alert warn">Market context: {{ tradeQualityDimensionMarketContext.reason || "pending per-trade BTC/regime evidence." }}</div>
                </div>
              </article>
            </div>
          </div>
        </article>

        <article v-if="showLegacyTradeQualityPanels" class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Entry Quality Attribution</div>
            <div class="title-actions">
              <span class="tag warn">diagnostic only</span>
              <span class="tag blue">feature {{ formatPct(tradeQualityEntryQualityAttribution.feature_coverage) }}</span>
            </div>
          </div>
          <div class="panel-body">
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">Feature Coverage</div><div class="kpi-value flat">{{ formatPct(tradeQualityEntryQualityAttribution.feature_coverage) }}</div><div class="kpi-sub">{{ tradeQualityEntryQualityAttribution.feature_covered_count || 0 }} / {{ tradeQualityEntryQualityAttribution.sample_count || 0 }} samples</div></article>
              <article class="card kpi"><div class="kpi-label">Top Bad Entry</div><div class="kpi-value warn">{{ tradeQualityEntryQualityAttribution.top_bad_entry_pattern || "-" }}</div><div class="kpi-sub">largest loss pattern</div></article>
              <article class="card kpi"><div class="kpi-label">Last Backfill</div><div class="kpi-value flat">{{ tradeQualityEntryFeatureLast?.candidate_count ?? "-" }}</div><div class="kpi-sub">{{ tradeQualityEntryFeatureLast?.mode || "not run" }} · {{ tradeQualityEntryFeatureLast?.updated_count || 0 }} updated</div></article>
              <article class="card kpi"><div class="kpi-label">Missing Candle</div><div class="kpi-value down">{{ tradeQualityEntryFeatureLast?.missing_candle_count || 0 }}</div><div class="kpi-sub">entry-feature window gaps</div></article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Entry Label</th><th>Meaning</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Score</th><th>Optimization</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityEntryQualityRows" :key="row.label" @click="selectTradeQualityEntryLabel(row.label)">
                    <td><span class="tag" :class="statusClass(row.label)">{{ row.label }}</span></td>
                    <td>{{ row.meaning }}</td>
                    <td>{{ row.trade_count }}</td>
                    <td>{{ row.loss_count }}</td>
                    <td>{{ formatPct(row.win_rate) }}</td>
                    <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                    <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                    <td>{{ signedR(row.avg_MFE_R) }}</td>
                    <td>{{ signedR(row.avg_MAE_R) }}</td>
                    <td>{{ ratioX(row.avg_score) }}</td>
                    <td class="reason-cell">{{ row.optimization }}</td>
                  </tr>
                  <tr v-if="!tradeQualityEntryQualityRows.length"><td colspan="11" class="empty-state">No entry-quality attribution yet. Run Entry Feature Backfill for the selected package.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article v-if="showLegacyTradeQualityPanels" class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Entry Microstructure Attribution</div>
            <div class="title-actions">
              <span class="tag warn">diagnostic only</span>
              <span class="tag blue">coverage {{ formatPct(tradeQualityEntryMicroAttribution.coverage) }}</span>
            </div>
          </div>
          <div class="panel-body">
            <div class="notice subtle">
              Microstructure attribution is split by strategy_line. without_micro does not require micro evidence; micro_fast/full evidence gaps are tracked separately.
            </div>
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">Micro Coverage</div><div class="kpi-value flat">{{ formatPct(tradeQualityEntryMicroAttribution.coverage) }}</div><div class="kpi-sub">{{ tradeQualityEntryMicroAttribution.feature_covered_count || 0 }} / {{ tradeQualityEntryMicroAttribution.sample_count || 0 }} samples</div></article>
              <article class="card kpi"><div class="kpi-label">Top V2 Label</div><div class="kpi-value warn">{{ tradeQualityEntryMicroAttribution.top_label || "-" }}</div><div class="kpi-sub">largest loss pattern</div></article>
              <article class="card kpi"><div class="kpi-label">Last Backfill</div><div class="kpi-value flat">{{ tradeQualityEntryMicroLast?.candidate_count ?? "-" }}</div><div class="kpi-sub">{{ tradeQualityEntryMicroLast?.mode || "not run" }} · {{ tradeQualityEntryMicroLast?.updated_count || 0 }} updated</div></article>
              <article class="card kpi"><div class="kpi-label">Evidence Status</div><div class="kpi-value flat">{{ Object.keys(tradeQualityEntryMicroLast?.evidence_status_counts || {}).length || "-" }}</div><div class="kpi-sub">status buckets</div></article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>V2 Label</th><th>Meaning</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Score</th><th>Optimization</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityEntryMicroRows" :key="row.label" @click="selectTradeQualityEntryV2Label(row.label)">
                    <td><span class="tag" :class="statusClass(row.label)">{{ row.label }}</span></td>
                    <td>{{ row.meaning }}</td>
                    <td>{{ row.trade_count }}</td>
                    <td>{{ row.loss_count }}</td>
                    <td>{{ formatPct(row.win_rate) }}</td>
                    <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                    <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                    <td>{{ signedR(row.avg_MFE_R) }}</td>
                    <td>{{ signedR(row.avg_MAE_R) }}</td>
                    <td>{{ ratioX(row.avg_acceptance_score) }}</td>
                    <td class="reason-cell">{{ row.optimization }}</td>
                  </tr>
                  <tr v-if="!tradeQualityEntryMicroRows.length"><td colspan="11" class="empty-state">No microstructure attribution yet. Run Fill Microstructure for the selected package.</td></tr>
                </tbody>
              </table>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Strategy Line</th><th>Samples</th><th>Coverage</th><th>V2 Labels</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityEntryMicroStrategyRows" :key="row.strategy_line">
                    <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                    <td>{{ row.sample_count }}</td>
                    <td>{{ formatPct(row.coverage) }}</td>
                    <td class="reason-cell">{{ (row.items || []).map((item) => `${item.label}:${item.trade_count}`).join(" · ") }}</td>
                  </tr>
                  <tr v-if="!tradeQualityEntryMicroStrategyRows.length"><td colspan="4" class="empty-state">No strategy-line split yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Entry Market Context</div>
            <div class="title-actions">
              <span class="tag warn">diagnostic only</span>
              <span class="tag blue">coverage {{ formatPct(tradeQualityEntryMarketAttribution.coverage) }}</span>
            </div>
          </div>
          <div class="panel-body">
            <div class="notice subtle">
              Market context is shared by all strategy lines. OI, funding, and BTC alignment are attached at entry_time and stay shadow-only.
            </div>
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">Market Coverage</div><div class="kpi-value flat">{{ formatPct(tradeQualityEntryMarketAttribution.coverage) }}</div><div class="kpi-sub">{{ tradeQualityEntryMarketAttribution.feature_covered_count || 0 }} / {{ tradeQualityEntryMarketAttribution.sample_count || 0 }} samples</div></article>
              <article class="card kpi"><div class="kpi-label">Top Context</div><div class="kpi-value warn">{{ tradeQualityEntryMarketAttribution.top_label || "-" }}</div><div class="kpi-sub">largest market-context loss bucket</div></article>
              <article class="card kpi"><div class="kpi-label">Funding Buckets</div><div class="kpi-value flat">{{ tradeQualityMarketContextAggregates.length || 0 }}</div><div class="kpi-sub">available through filters</div></article>
              <article class="card kpi"><div class="kpi-label">V3 Ready</div><div class="kpi-value flat">{{ tradeQualityEntryV3Attribution.sample_count || 0 }}</div><div class="kpi-sub">merged context samples</div></article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Market Label</th><th>Meaning</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Score</th><th>Optimization</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityEntryMarketRows" :key="row.label" @click="selectTradeQualityMarketContextLabel(row.label)">
                    <td><span class="tag" :class="statusClass(row.label)">{{ row.label }}</span></td>
                    <td>{{ row.meaning }}</td>
                    <td>{{ row.trade_count }}</td>
                    <td>{{ row.loss_count }}</td>
                    <td>{{ formatPct(row.win_rate) }}</td>
                    <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                    <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                    <td>{{ signedR(row.avg_MFE_R) }}</td>
                    <td>{{ signedR(row.avg_MAE_R) }}</td>
                    <td>{{ ratioX(row.avg_score) }}</td>
                    <td class="reason-cell">{{ row.optimization }}</td>
                  </tr>
                  <tr v-if="!tradeQualityEntryMarketRows.length"><td colspan="11" class="empty-state">No market-context attribution yet. Use Refresh & Enrich for the selected package.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Entry Context V3 Attribution</div>
            <div class="title-actions">
              <span class="tag warn">shadow only</span>
              <span class="tag blue">coverage {{ formatPct(tradeQualityEntryV3Attribution.coverage) }}</span>
            </div>
          </div>
          <div class="panel-body">
            <div class="notice subtle">
              V3 merges price-entry features, market OI/funding/BTC context, and micro CVD/OFI/spread/depth when the strategy line has micro evidence.
            </div>
            <div class="grid-kpi compact-kpi">
              <article class="card kpi"><div class="kpi-label">V3 Coverage</div><div class="kpi-value flat">{{ formatPct(tradeQualityEntryV3Attribution.coverage) }}</div><div class="kpi-sub">{{ tradeQualityEntryV3Attribution.feature_covered_count || 0 }} / {{ tradeQualityEntryV3Attribution.sample_count || 0 }} samples</div></article>
              <article class="card kpi"><div class="kpi-label">Top V3 Label</div><div class="kpi-value warn">{{ tradeQualityEntryV3Attribution.top_label || "-" }}</div><div class="kpi-sub">merged quality diagnosis</div></article>
              <article class="card kpi"><div class="kpi-label">Strategy Splits</div><div class="kpi-value flat">{{ tradeQualityEntryV3StrategyRows.length || 0 }}</div><div class="kpi-sub">mixed package safe</div></article>
              <article class="card kpi"><div class="kpi-label">Filtered Rows</div><div class="kpi-value flat">{{ tradeQualityEntryV3Aggregates.length || 0 }}</div><div class="kpi-sub">aggregate buckets</div></article>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>V3 Label</th><th>Meaning</th><th>Trades</th><th>Loss</th><th>Win</th><th>Avg R</th><th>Total R</th><th>MFE</th><th>MAE</th><th>Score</th><th>Optimization</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityEntryV3Rows" :key="row.label" @click="selectTradeQualityEntryV3Label(row.label)">
                    <td><span class="tag" :class="statusClass(row.label)">{{ row.label }}</span></td>
                    <td>{{ row.meaning }}</td>
                    <td>{{ row.trade_count }}</td>
                    <td>{{ row.loss_count }}</td>
                    <td>{{ formatPct(row.win_rate) }}</td>
                    <td :class="pnlClass(row.avg_R)">{{ signedR(row.avg_R) }}</td>
                    <td :class="pnlClass(row.total_R)">{{ signedR(row.total_R) }}</td>
                    <td>{{ signedR(row.avg_MFE_R) }}</td>
                    <td>{{ signedR(row.avg_MAE_R) }}</td>
                    <td>{{ ratioX(row.avg_score) }}</td>
                    <td class="reason-cell">{{ row.optimization }}</td>
                  </tr>
                  <tr v-if="!tradeQualityEntryV3Rows.length"><td colspan="11" class="empty-state">No V3 attribution yet. Use Refresh & Enrich for the selected package.</td></tr>
                </tbody>
              </table>
            </div>
            <div class="audit-symbol-table compact-table">
              <table>
                <thead><tr><th>Strategy Line</th><th>Samples</th><th>Coverage</th><th>V3 Labels</th></tr></thead>
                <tbody>
                  <tr v-for="row in tradeQualityEntryV3StrategyRows" :key="row.strategy_line">
                    <td><span class="tag blue">{{ row.strategy_line }}</span></td>
                    <td>{{ row.sample_count }}</td>
                    <td>{{ formatPct(row.coverage) }}</td>
                    <td class="reason-cell">{{ (row.items || []).map((item) => `${item.label}:${item.trade_count}`).join(" · ") }}</td>
                  </tr>
                  <tr v-if="!tradeQualityEntryV3StrategyRows.length"><td colspan="4" class="empty-state">No V3 strategy-line split yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Root Cause Pareto</div><span class="tag blue">{{ tradeQualityRootCauses.length }} causes</span></div>
            <div class="panel-body table-scroll">
              <table><thead><tr><th>Root Cause</th><th>Trades</th><th>Avg R</th><th>Median R</th><th>Win</th><th>MFE</th><th>MAE</th></tr></thead><tbody>
                <tr v-for="row in tradeQualityRootCauses" :key="row.key">
                  <td><span class="tag" :class="statusClass(row.key)">{{ row.key }}</span></td>
                  <td>{{ row.sample_count }}</td>
                  <td :class="pnlClass(row.avg_net_R)">{{ signedR(row.avg_net_R) }}</td>
                  <td :class="pnlClass(row.median_net_R)">{{ signedR(row.median_net_R) }}</td>
                  <td>{{ formatPct(row.win_rate) }}</td>
                  <td>{{ signedR(row.avg_MFE_R) }}</td>
                  <td>{{ signedR(row.avg_MAE_R) }}</td>
                </tr>
                <tr v-if="!tradeQualityRootCauses.length"><td colspan="7" class="empty-state">No trade quality samples yet.</td></tr>
              </tbody></table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Quality Tags</div><span class="tag blue">{{ tradeQualityByTag.length }} tags</span></div>
            <div class="panel-body table-scroll">
              <table><thead><tr><th>Tag</th><th>Trades</th><th>Avg R</th><th>Win</th><th>MFE</th><th>MAE</th></tr></thead><tbody>
                <tr v-for="row in tradeQualityByTag" :key="row.key">
                  <td><span class="tag" :class="statusClass(row.key)">{{ row.key }}</span></td>
                  <td>{{ row.sample_count }}</td>
                  <td :class="pnlClass(row.avg_net_R)">{{ signedR(row.avg_net_R) }}</td>
                  <td>{{ formatPct(row.win_rate) }}</td>
                  <td>{{ signedR(row.avg_MFE_R) }}</td>
                  <td>{{ signedR(row.avg_MAE_R) }}</td>
                </tr>
                <tr v-if="!tradeQualityByTag.length"><td colspan="6" class="empty-state">No quality tags yet.</td></tr>
              </tbody></table>
            </div>
          </article>
        </div>

        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Strategy / Side</div><span class="tag blue">comparison</span></div>
            <div class="panel-body table-scroll">
              <table><thead><tr><th>Dimension</th><th>Key</th><th>Trades</th><th>Avg R</th><th>Win</th><th>MFE</th><th>MAE</th></tr></thead><tbody>
                <tr v-for="row in tradeQualityStrategySideRows" :key="`${row.dimension}-${row.key}`">
                  <td>{{ row.dimension }}</td><td>{{ row.key }}</td><td>{{ row.sample_count }}</td>
                  <td :class="pnlClass(row.avg_net_R)">{{ signedR(row.avg_net_R) }}</td><td>{{ formatPct(row.win_rate) }}</td><td>{{ signedR(row.avg_MFE_R) }}</td><td>{{ signedR(row.avg_MAE_R) }}</td>
                </tr>
              </tbody></table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Symbol / Exit Reason</div><span class="tag blue">{{ tradeQualityBySymbol.length + tradeQualityByExitReason.length }} rows</span></div>
            <div class="panel-body table-scroll">
              <table><thead><tr><th>Dimension</th><th>Key</th><th>Trades</th><th>Avg R</th><th>Win</th><th>Root Causes</th></tr></thead><tbody>
                <tr v-for="row in [...tradeQualityByExitReason, ...tradeQualityBySymbol]" :key="`${row.dimension}-${row.key}`">
                  <td>{{ row.dimension }}</td><td>{{ row.key }}</td><td>{{ row.sample_count }}</td><td :class="pnlClass(row.avg_net_R)">{{ signedR(row.avg_net_R) }}</td><td>{{ formatPct(row.win_rate) }}</td>
                  <td class="reason-cell">{{ Object.entries(row.evidence?.root_cause_counts || {}).map(([k,v]) => `${k}:${v}`).join(", ") }}</td>
                </tr>
              </tbody></table>
            </div>
          </article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Full Trade Records</div>
            <div class="title-actions">
              <button class="btn tiny" :disabled="!tradeQualityCanPrev || tradeQualityDetailsLoading" @click="tradeQualityPrevPage">Previous</button>
              <span class="tag blue">{{ tradeQualitySamples.length }} / {{ tradeQuality.total || tradeQualitySamples.length }} rows · offset {{ tradeQualityFilters.offset || 0 }}</span>
              <button class="btn tiny" :disabled="!tradeQualityCanNext || tradeQualityDetailsLoading" @click="tradeQualityNextPage">Next</button>
            </div>
          </div>
          <div class="panel-body table-scroll">
            <table><thead><tr><th>trade_id</th><th>source</th><th>strategy</th><th>symbol</th><th>side</th><th>entry_time</th><th>exit_time</th><th>entry</th><th>exit</th><th>hold</th><th>gross</th><th>fee</th><th>net</th><th>net_R</th><th>SL</th><th>TP</th><th>RR</th><th>MFE_R</th><th>MAE_R</th><th>entry_type</th><th>exit_reason</th><th>entry quality</th><th>market</th><th>V3</th><th>coverage</th><th>root</th></tr></thead><tbody>
              <tr v-for="row in tradeQualitySamples" :key="row.diagnostic_id" @click="selectTradeQualitySample(row)">
                <td class="path">{{ row.trade_id }}</td>
                <td><span class="tag" :class="statusClass(row.source)">{{ row.source }}</span></td>
                <td><span class="tag blue">{{ row.strategy_line || "-" }}</span></td>
                <td>{{ row.symbol }}</td>
                <td>{{ row.side }}</td>
                <td class="path">{{ row.entry_time || "-" }}</td>
                <td class="path">{{ row.exit_time || "-" }}</td>
                <td>{{ price(row.entry_price) }}</td>
                <td>{{ price(row.exit_price) }}</td>
                <td>{{ money(row.holding_minutes) }}m</td>
                <td :class="pnlClass(row.gross_pnl)">{{ money(row.gross_pnl) }}</td>
                <td>{{ money(row.fee) }}</td>
                <td :class="pnlClass(row.net_pnl)">{{ money(row.net_pnl) }}</td>
                <td :class="pnlClass(row.net_R)">{{ signedR(row.net_R) }}</td>
                <td>{{ price(row.planned_SL) }}</td>
                <td>{{ price(row.planned_TP) }}</td>
                <td>{{ money(row.planned_RR) }}</td>
                <td>{{ signedR(row.MFE_R) }}</td>
                <td>{{ signedR(row.MAE_R) }}</td>
                <td>{{ row.entry_type || "-" }}</td>
                <td>{{ row.exit_reason || "-" }}</td>
                <td><span class="tag" :class="statusClass(row.entry_quality_label || row.entry_features?.entry_quality_label)">{{ row.entry_quality_label || row.entry_features?.entry_quality_label || "-" }}</span></td>
                <td><span class="tag" :class="statusClass(row.entry_market_context?.market_context_label)">{{ row.entry_market_context?.market_context_label || "-" }}</span></td>
                <td><span class="tag" :class="statusClass(row.entry_context_v3?.entry_context_v3_label)">{{ row.entry_context_v3?.entry_context_v3_label || "-" }}</span></td>
                <td>{{ row.entry_feature_coverage || row.entry_features?.entry_feature_coverage || "-" }}</td>
                <td><span class="tag" :class="statusClass(row.root_cause)">{{ row.root_cause }}</span></td>
              </tr>
              <tr v-if="!tradeQualitySamples.length"><td colspan="26" class="empty-state">No matching diagnostic trade samples.</td></tr>
            </tbody></table>
          </div>
        </article>

        <article v-if="tradeQualitySelectedSample" class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Selected Trade Detail</div>
            <span class="tag blue">{{ tradeQualitySelectedSample.trade_id }}</span>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">lineage</div><div class="plan-num flat">{{ tradeQualitySelectedSample.run_id || "-" }}</div><div class="kpi-sub">{{ tradeQualitySelectedSample.cycle_id || "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">replay</div><div class="plan-num flat">{{ tradeQualitySelectedSample.replay_status || "-" }}</div><div class="kpi-sub">{{ tradeQualitySelectedSample.excursion_model || "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">direction</div><div class="plan-num flat">{{ tradeQualitySelectedSample.direction_quality || "-" }}</div><div class="kpi-sub">entry {{ tradeQualitySelectedSample.entry_quality || "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">SL / TP</div><div class="plan-num flat">{{ tradeQualitySelectedSample.sl_quality || "-" }}</div><div class="kpi-sub">tp {{ tradeQualitySelectedSample.tp_quality || "-" }}</div></div>
            </div>
            <div class="notice-list">
              <div class="notice"><span>quality tags</span><span class="tag blue">{{ (tradeQualitySelectedSample.quality_tags || []).join(", ") || "-" }}</span></div>
              <div class="notice"><span>archive</span><span class="tag blue">{{ tradeQualitySelectedSample.archive_id || "-" }}</span></div>
              <div class="notice"><span>entry features</span><small>{{ shortJson(tradeQualitySelectedSample.entry_features || {}, 600) }}</small></div>
              <div class="notice"><span>evidence</span><small>{{ shortJson(tradeQualitySelectedSample.evidence || {}, 600) }}</small></div>
            </div>
          </div>
        </article>
      </section>

      <section v-else-if="activePage === 'backtest-lab'">
        <PageHeader title="Backtest Lab" subtitle="P21 V2: 30d all-universe Kline config matrix backtest for strategy1 / strategy4 / strategy5 / strategy6. No runtime side effects." />
        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Config Matrix Experiment</div>
            <div class="title-actions">
              <span class="tag" :class="backtestLabLoading ? 'warn' : 'good'">{{ backtestLabLoading ? backtestLabAction || "running" : "idle" }}</span>
              <span v-if="backtestLabMessage" class="tag blue">{{ backtestLabMessage }}</span>
            </div>
          </div>
          <div class="panel-body form-grid">
            <label>strategy line
              <select v-model="backtestLabFilters.strategy_line">
                <option value="all">all: strategy1 / 4 / 5</option>
                <option value="without_micro">strategy1 without_micro</option>
                <option value="strategy4">strategy4 wait observe</option>
                <option value="strategy5">strategy5 direction engine</option>
                <option value="strategy6">strategy6 market accepted entry</option>
              </select>
            </label>
            <label>days<input v-model.number="backtestLabFilters.days" type="number" min="1" max="90" /></label>
            <label>max symbols<input v-model.number="backtestLabFilters.max_symbols" type="number" min="1" max="600" /></label>
            <label>max parameter sets<input v-model.number="backtestLabFilters.max_sets" type="number" min="1" max="5000" /></label>
            <label>scheduler
              <select v-model="backtestLabFilters.scheduler_mode">
                <option value="parameter_batch">parameter batch</option>
                <option value="global_queue">global queue</option>
              </select>
            </label>
            <label>shard size<input v-model.number="backtestLabFilters.symbol_shard_size" type="number" min="1" max="200" /></label>
            <label>workers<input v-model.number="backtestLabFilters.max_workers" type="number" min="1" max="32" /></label>
            <label>symbols override<input v-model="backtestLabFilters.symbols" placeholder="BTCUSDT,ETHUSDT optional" /></label>
            <div class="action-row">
              <button class="btn" :disabled="backtestLabLoading" @click="loadBacktestLabPage"><RefreshCw />Refresh</button>
              <button class="btn primary" :disabled="backtestLabLoading || isBacktestLabJobActive()" @click="startBacktestLabJob('kline_download')"><DatabaseZap />Start Kline Job</button>
              <button class="btn cycle" :disabled="backtestLabLoading || isBacktestLabJobActive()" @click="startBacktestLabJob('matrix_backtest')"><Play />Start Matrix Job</button>
              <button class="btn danger" :disabled="!isBacktestLabJobActive()" @click="stopBacktestLabJob"><Square />Stop Job</button>
            </div>
          </div>
        </article>

        <div class="kpi-grid">
          <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Kline Ready</span></div><div class="kpi-value">{{ backtestLabKlineStatus?.ready_count ?? 0 }}/{{ backtestLabKlineStatus?.count ?? 0 }}</div><div class="kpi-sub">30d 1m cache coverage</div></article>
          <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Matrix Contracts</span></div><div class="kpi-value">{{ backtestLabMatrixContract?.parameter_set_count ?? 0 }}</div><div class="kpi-sub">config-writable parameter sets</div></article>
          <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Best Matrix PF</span></div><div class="kpi-value" :class="Number(backtestLabMatrix?.best?.metrics?.profit_factor || 0) > 1 ? 'pos' : 'warn-text'">{{ backtestLabMatrix?.best?.metrics?.profit_factor ?? "-" }}</div><div class="kpi-sub">{{ backtestLabMatrix?.best?.parameter_set_id || "no experiment" }}</div></article>
          <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Target</span></div><div class="kpi-value">PF &gt; 1</div><div class="kpi-sub">export candidate only</div></article>
        </div>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Long-Run Job Progress</div>
            <div class="title-actions">
              <span class="tag" :class="['running','running_degraded','stalled'].includes(backtestLabJob?.status) ? 'warn' : backtestLabJob?.status === 'done' ? 'good' : 'blue'">{{ backtestLabJob?.status || "no job" }}</span>
              <span class="tag blue">{{ backtestLabJobProgress().engine_mode || "offline_real_evaluator" }}</span>
              <span v-if="backtestLabJob?.synthetic" class="tag warn">script job</span>
              <span v-if="backtestLabJobProgress().health_reason_codes?.length" class="tag warn">{{ backtestLabJobProgress().health_reason_codes.join(", ") }}</span>
            </div>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">job</div><div class="plan-num flat">{{ backtestLabJob?.job_id || "-" }}</div><div class="kpi-sub">{{ backtestLabJob?.job_type || "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">phase</div><div class="plan-num flat">{{ backtestLabJob?.phase || backtestLabJobProgress().phase || "-" }}</div><div class="kpi-sub">{{ backtestLabJobProgress().current_strategy_line || "all lines" }}</div></div>
              <div class="plan-box"><div class="plan-name">progress</div><div class="plan-num flat">{{ backtestLabProgressPct() }}%</div><div class="kpi-sub">{{ backtestLabJobProgress().done_count || 0 }} / {{ backtestLabJobProgress().total_count || 0 }}</div></div>
              <div class="plan-box"><div class="plan-name">current</div><div class="plan-num flat">{{ backtestLabJobProgress().current_symbol || "-" }}</div><div class="kpi-sub">{{ backtestLabJobProgress().current_parameter_set_id || "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">workers</div><div class="plan-num flat">{{ backtestLabJobProgress().active_workers ?? backtestLabJob?.health?.worker_active_count ?? 0 }}/{{ backtestLabJobProgress().max_workers ?? backtestLabJob?.request?.max_workers ?? 0 }}</div><div class="kpi-sub">idle {{ backtestLabJobProgress().idle_workers ?? "-" }} · pids {{ (backtestLabJobProgress().worker_pids || backtestLabJob?.health?.worker_pids || []).length }}</div></div>
              <div class="plan-box"><div class="plan-name">scheduler</div><div class="plan-num flat">{{ backtestLabJobProgress().execution_mode || backtestLabJob?.request?.scheduler_mode || "-" }}</div><div class="kpi-sub">{{ backtestLabJobProgress().memory_guard_status || "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">shard sec</div><div class="plan-num flat">{{ backtestLabJobProgress().avg_shard_sec ?? "-" }}</div><div class="kpi-sub">p95 {{ backtestLabJobProgress().p95_shard_sec ?? "-" }}</div></div>
              <div class="plan-box"><div class="plan-name">heartbeat</div><div class="plan-num flat">{{ backtestLabJobProgress().stalled_sec ?? backtestLabJob?.health?.stalled_sec ?? "-" }}s</div><div class="kpi-sub">progress age {{ backtestLabJobProgress().progress_age_sec ?? backtestLabJob?.health?.progress_age_sec ?? "-" }}s</div></div>
            </div>
            <div class="progress-track"><div class="progress-fill" :style="{ width: `${backtestLabProgressPct()}%` }"></div></div>
            <div v-if="backtestLabJob?.last_error" class="notice warn"><span>last error</span><small>{{ backtestLabJob.last_error }}</small></div>
            <div v-if="backtestLabJob?.health?.reason_codes?.length" class="notice warn"><span>job health</span><small>{{ backtestLabJob.health.reason_codes.join(", ") }}</small></div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Strategy4 Persistent Observe Replay</div>
            <div class="title-actions">
              <span class="tag good">real observe replay</span>
              <span class="tag blue">legacy wait-band hidden</span>
              <button class="btn" :disabled="backtestLabLoading" @click="loadBacktestLabPage"><RefreshCw />Refresh</button>
              <button class="btn cycle" :disabled="backtestLabLoading || isBacktestLabJobActive()" @click="runBacktestLabStrategy4Replay"><Play />Run Small Replay</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="plan-grid">
              <div class="plan-box"><div class="plan-name">pool</div><div class="plan-num flat">{{ Object.values(backtestLabStrategy4Replay?.pool_counts || {}).reduce((a,b) => Number(a) + Number(b), 0) }}</div><div class="kpi-sub">admitted observe items</div></div>
              <div class="plan-box"><div class="plan-name">attempts</div><div class="plan-num flat">{{ backtestLabStrategy4Replay?.attempt_count ?? 0 }}</div><div class="kpi-sub">5m replay rechecks</div></div>
              <div class="plan-box"><div class="plan-name">executable</div><div class="plan-num flat">{{ backtestLabStrategy4Replay?.executable_attempt_count ?? 0 }}</div><div class="kpi-sub">paper-style candidates</div></div>
              <div class="plan-box"><div class="plan-name">latest run</div><div class="plan-num flat">{{ backtestLabStrategy4ReplayRun?.experiment_id || "-" }}</div><div class="kpi-sub">{{ backtestLabStrategy4Replay?.strategy4_replay_mode || "strategy4_persistent_observe_replay" }}</div></div>
            </div>
            <div class="wide-grid">
              <div class="table-scroll">
                <h3>Observe Pool</h3>
                <table><thead><tr><th>Symbol</th><th>Status</th><th>Attempts</th><th>Original</th><th>Current</th><th>Reasons</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabStrategy4ReplayPool.pool || []).slice(0, 20)" :key="row.pool_id">
                    <td>{{ row.symbol }}</td><td><span class="tag" :class="statusClass(row.status)">{{ row.status }}</span></td><td>{{ row.attempt_count }}</td><td>{{ row.original_side || "-" }}</td><td>{{ row.current_side || "-" }} <span v-if="row.side_changed" class="tag warn">changed</span></td><td class="reason-cell">{{ (row.latest_reason_codes || row.source_reason_codes || []).join(", ") }}</td>
                  </tr>
                  <tr v-if="!(backtestLabStrategy4ReplayPool.pool || []).length"><td colspan="6" class="empty-state">No strategy4 replay pool rows yet.</td></tr>
                </tbody></table>
              </div>
              <div class="table-scroll">
                <h3>Attempt Timeline</h3>
                <table><thead><tr><th>Symbol</th><th>#</th><th>Decision</th><th>Action</th><th>Exec</th><th>Reasons</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabStrategy4ReplayAttempts.attempts || []).slice(0, 20)" :key="row.attempt_id">
                    <td>{{ row.symbol }}</td><td>{{ row.attempt_index }}</td><td>{{ row.current_side || row.decision }}</td><td>{{ row.action || "-" }}</td><td><span class="tag" :class="row.executable ? 'good' : 'blue'">{{ row.executable ? "yes" : "no" }}</span></td><td class="reason-cell">{{ (row.reason_codes || []).join(", ") }}</td>
                  </tr>
                  <tr v-if="!(backtestLabStrategy4ReplayAttempts.attempts || []).length"><td colspan="6" class="empty-state">No strategy4 replay attempts yet.</td></tr>
                </tbody></table>
              </div>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Matrix Leaderboard</div><span class="tag blue">{{ backtestLabMatrix?.parameter_set_count || 0 }} sets</span></div>
          <div class="panel-body table-scroll">
            <table><thead><tr><th>Rank</th><th>Parameter Set</th><th>Line</th><th>PF</th><th>Expectancy</th><th>Win Rate</th><th>Trades</th><th>Total R</th><th>Max DD</th><th>Config</th><th>Quality</th></tr></thead><tbody>
              <tr v-for="(row, idx) in (backtestLabMatrix?.leaderboard || backtestLabLeaderboard?.leaderboard || []).slice(0, 50)" :key="`${row.experiment_id}-${row.parameter_set_id}`">
                <td>{{ idx + 1 }}</td>
                <td><button class="link-btn" @click="openBacktestLabExperiment(row.experiment_id)">{{ row.parameter_set_id }}</button></td>
                <td>{{ row.strategy_line || row.parameters?.strategy_line }}</td>
                <td :class="Number(row.metrics?.profit_factor || 0) > 1 ? 'pos' : 'neg'">{{ row.metrics?.profit_factor ?? "-" }}</td>
                <td>{{ signedR(row.metrics?.expectancy_R) }}</td>
                <td>{{ formatPct(row.metrics?.win_rate) }}</td>
                <td>{{ row.metrics?.trade_count || row.metrics?.accepted_count }}</td>
                <td>{{ signedR(row.metrics?.total_R) }}</td>
                <td>{{ signedR(row.metrics?.max_drawdown_R) }}</td>
                <td><small>score {{ row.parameters?.min_score }} / RR {{ row.parameters?.target_rr }} / {{ row.parameters?.tp_target_policy?.mode }}</small></td>
                <td><button class="link-btn" @click="openBacktestLabQuality(row)">Quality</button></td>
              </tr>
              <tr v-if="!(backtestLabMatrix?.leaderboard || backtestLabLeaderboard?.leaderboard || []).length"><td colspan="11" class="empty-state">Run Matrix to generate config candidates.</td></tr>
            </tbody></table>
          </div>
        </article>

        <article v-if="backtestLabQualitySelection" class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Backtest Trade Quality</div>
            <div class="action-row tight">
              <span class="tag blue">{{ backtestLabQualitySelection.parameter_set_id || backtestLabQualitySelection.experiment_id }}</span>
              <button class="btn" :disabled="backtestLabQualityLoading" @click="materializeBacktestLabQuality(true)"><RefreshCw />Dry Run</button>
              <button class="btn cycle" :disabled="backtestLabQualityLoading" @click="materializeBacktestLabQuality(false)"><DatabaseZap />Materialize</button>
            </div>
          </div>
          <div class="panel-body">
            <div v-if="backtestLabQualityDryRun" class="notice">
              <span>{{ backtestLabQualityDryRun.dry_run ? "dry-run" : "materialized" }} · selected {{ backtestLabQualityDryRun.selected_order_count || 0 }} orders · samples {{ backtestLabQualityDryRun.materialized_count || 0 }}</span>
              <span class="tag blue">{{ backtestLabQualityDryRun.source }}</span>
            </div>
            <div class="kpi-grid">
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Samples</span></div><div class="kpi-value">{{ backtestLabQualitySummary?.total ?? 0 }}</div><div class="kpi-sub">backtest_p21_v2 only</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Win Rate</span></div><div class="kpi-value">{{ formatPct(backtestLabQualitySummary?.summary?.performance_stats?.win_rate) }}</div><div class="kpi-sub">R-first diagnostics</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Expectancy</span></div><div class="kpi-value" :class="pnlClass(backtestLabQualitySummary?.summary?.performance_stats?.expectancy_R)">{{ signedR(backtestLabQualitySummary?.summary?.performance_stats?.expectancy_R) }}</div><div class="kpi-sub">per trade R</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Replay</span></div><div class="kpi-value">{{ formatPct(backtestLabQualitySummary?.summary?.replay_coverage?.ratio) }}</div><div class="kpi-sub">1m MFE/MAE coverage</div></article>
            </div>
            <div class="wide-grid">
              <div class="table-scroll">
                <h3>Root Cause</h3>
                <table><thead><tr><th>Cause</th><th>Count</th><th>Loss</th><th>Avg R</th><th>MFE</th><th>MAE</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabQualitySummary?.summary?.root_cause_attribution?.items || []).slice(0, 12)" :key="row.root_cause">
                    <td><span class="tag blue">{{ row.root_cause }}</span></td><td>{{ row.count }}</td><td>{{ row.loss_count }}</td><td :class="pnlClass(row.avg_net_R)">{{ signedR(row.avg_net_R) }}</td><td>{{ signedR(row.avg_MFE_R) }}</td><td>{{ signedR(row.avg_MAE_R) }}</td>
                  </tr>
                </tbody></table>
              </div>
              <div class="table-scroll">
                <h3>Rollups</h3>
                <table><thead><tr><th>Dimension</th><th>Key</th><th>Samples</th><th>Avg R</th><th>Win</th><th>MFE</th><th>MAE</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabQualityAggregates?.aggregates || []).slice(0, 30)" :key="row.rollup_id">
                    <td>{{ row.dimension }}</td><td>{{ row.key }}</td><td>{{ row.sample_count }}</td><td :class="pnlClass(row.metrics?.avg_net_R)">{{ signedR(row.metrics?.avg_net_R) }}</td><td>{{ formatPct(row.metrics?.win_rate) }}</td><td>{{ signedR(row.metrics?.avg_MFE_R) }}</td><td>{{ signedR(row.metrics?.avg_MAE_R) }}</td>
                  </tr>
                </tbody></table>
              </div>
            </div>
            <div class="table-scroll">
              <h3>Diagnostic Samples</h3>
              <table><thead><tr><th>Symbol</th><th>Line</th><th>Side</th><th>Entry</th><th>Exit</th><th>Net R</th><th>MFE</th><th>MAE</th><th>Root Cause</th></tr></thead><tbody>
                <tr v-for="row in (backtestLabQualitySamples?.samples || []).slice(0, 80)" :key="row.diagnostic_id">
                  <td>{{ row.symbol }}</td><td>{{ row.strategy_line }}</td><td>{{ row.side }}</td><td>{{ row.entry_time }}</td><td>{{ row.exit_reason || row.exit_time }}</td><td :class="pnlClass(row.net_R)">{{ signedR(row.net_R) }}</td><td>{{ signedR(row.MFE_R) }}</td><td>{{ signedR(row.MAE_R) }}</td><td><span class="tag blue">{{ row.root_cause }}</span></td>
                </tr>
                <tr v-if="!(backtestLabQualitySamples?.samples || []).length"><td colspan="9" class="empty-state">No materialized quality samples yet. Run dry-run, then materialize a bounded candidate package.</td></tr>
              </tbody></table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>TQ Materialization Queue</div>
            <div class="action-row tight">
              <span class="tag blue">{{ backtestLabTqJobs.count || 0 }} jobs</span>
              <button class="btn" :disabled="backtestLabTqJobLoading" @click="loadBacktestLabTqJobs"><RefreshCw />Refresh Jobs</button>
              <button class="btn cycle" :disabled="backtestLabTqJobLoading" @click="processNextBacktestLabTqJob"><DatabaseZap />Process Next</button>
            </div>
          </div>
          <div class="panel-body">
            <div v-if="backtestLabTqJobMessage" class="notice"><span>{{ backtestLabTqJobMessage }}</span><span class="tag warn">async V4/V5</span></div>
            <div class="table-scroll">
              <table><thead><tr><th>Job</th><th>Status</th><th>Stage</th><th>Progress</th><th>Experiment</th><th>Line</th><th>Param</th><th>TQ</th><th>V4</th><th>V5</th><th>Gates</th><th>Error</th></tr></thead><tbody>
                <tr v-for="row in (backtestLabTqJobs.jobs || []).slice(0, 20)" :key="row.job_id">
                  <td>{{ row.job_id }}</td>
                  <td><span class="tag" :class="row.status === 'done' || row.status === 'succeeded' ? 'good' : row.status === 'failed' ? 'bad' : 'blue'">{{ row.status }}</span></td>
                  <td>{{ row.stage || '-' }}</td>
                  <td>{{ row.progress_done || 0 }} / {{ row.progress_total || 0 }}</td>
                  <td>{{ row.experiment_id || row.request?.experiment_id || '-' }}</td>
                  <td>{{ row.strategy_line || row.request?.strategy_line || '-' }}</td>
                  <td>{{ row.parameter_set_id || row.request?.parameter_set_id || '-' }}</td>
                  <td>{{ row.result?.row_counts?.backtest_tq_samples ?? '-' }}</td>
                  <td>{{ row.result?.row_counts?.v4_entry_evidence ?? '-' }}</td>
                  <td>{{ row.result?.row_counts?.v5_causal_factors ?? '-' }}</td>
                  <td>{{ row.result?.row_counts?.v5_gate_candidates ?? '-' }}</td>
                  <td><small>{{ row.last_error || row.error || '-' }}</small></td>
                </tr>
                <tr v-if="!(backtestLabTqJobs.jobs || []).length"><td colspan="12" class="empty-state">No TQ async jobs yet. Materialize a backtest package to enqueue one.</td></tr>
              </tbody></table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Gate / Scoring Recommendation</div>
            <div class="action-row tight">
              <span class="tag warn">shadow only</span>
              <button class="btn" :disabled="backtestLabGateLoading" @click="loadBacktestLabGateScoring"><RefreshCw />Refresh</button>
              <button class="btn" :disabled="backtestLabGateLoading" @click="runBacktestLabGatePipeline(true)"><RefreshCw />Dry Run</button>
              <button class="btn cycle" :disabled="backtestLabGateLoading" @click="runBacktestLabGatePipeline(false)"><DatabaseZap />Build Candidates</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="form-grid">
              <label><span>Experiment</span><input v-model="backtestLabGateFilters.experiment_id" :placeholder="backtestLabGateExperimentId() || 'latest leaderboard experiment'" /></label>
              <label><span>Strategy</span><select v-model="backtestLabGateFilters.strategy_line"><option value="all">all</option><option value="without_micro">without_micro</option><option value="strategy4">strategy4</option><option value="strategy5">strategy5</option><option value="strategy6">strategy6</option></select></label>
              <label><span>Parameter Set</span><input v-model="backtestLabGateFilters.parameter_set_id" placeholder="optional" /></label>
              <label><span>Top N</span><input v-model.number="backtestLabGateFilters.top_n" type="number" min="1" max="30" /></label>
              <label><span>Rows</span><input v-model.number="backtestLabGateFilters.limit" type="number" min="1" max="100000" /></label>
              <label><span>Min Samples</span><input v-model.number="backtestLabGateFilters.min_samples" type="number" min="1" max="500" /></label>
              <label><span>Min Test PF</span><input v-model.number="backtestLabGateFilters.min_test_pf" type="number" min="0" step="0.1" /></label>
              <label><span>Min Coverage</span><input v-model.number="backtestLabGateFilters.min_coverage" type="number" min="0" max="1" step="0.01" /></label>
            </div>
            <div v-if="backtestLabGateMessage" class="notice">
              <span>{{ backtestLabGateMessage }}</span>
              <span class="tag blue">features {{ backtestLabGateFeatures.total || 0 }}</span>
            </div>
            <div class="kpi-grid">
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">TQ Samples</span></div><div class="kpi-value">{{ backtestLabGateBatch?.materialized_samples ?? backtestLabGateFeatureBuild?.feature_count ?? 0 }}</div><div class="kpi-sub">Top-N package materialized</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Buckets</span></div><div class="kpi-value">{{ backtestLabGateBucketBuild?.bucket_count ?? backtestLabGateBuckets.total ?? 0 }}</div><div class="kpi-sub">cost / side / session / market</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Scores</span></div><div class="kpi-value">{{ backtestLabGateScoreBuild?.score_count ?? backtestLabGateScores.total ?? 0 }}</div><div class="kpi-sub">monotonicity validation</div></article>
              <article class="kpi-card"><div class="kpi-top"><span class="kpi-label">Candidates</span></div><div class="kpi-value">{{ backtestLabGateCandidateBuild?.candidate_count ?? backtestLabGateCandidates.total ?? 0 }}</div><div class="kpi-sub">no config mutation</div></article>
            </div>
            <div class="wide-grid">
              <div class="table-scroll">
                <h3>Shadow Candidate Leaderboard</h3>
                <table><thead><tr><th>Rank</th><th>Candidate</th><th>Line</th><th>Status</th><th>PF Before</th><th>PF Test</th><th>Coverage</th><th>Risk</th><th>Patch</th></tr></thead><tbody>
                  <tr v-for="(row, idx) in (backtestLabGateCandidates.candidates || backtestLabGateRecommendations.recommendations || []).slice(0, 20)" :key="row.candidate_id || row.recommendation_id">
                    <td>{{ idx + 1 }}</td>
                    <td>{{ row.candidate_id || row.recommendation_id }}</td>
                    <td>{{ row.strategy_line }}</td>
                    <td><span class="tag" :class="row.status === 'shadow' ? 'blue' : 'warn'">{{ row.status || 'shadow' }}</span></td>
                    <td>{{ row.pf_before ?? row.test_metrics?.before?.profit_factor ?? row.train_metrics?.profit_factor ?? row.evidence?.baseline_profit_factor ?? '-' }}</td>
                    <td :class="Number(row.pf_after_test ?? row.test_metrics?.after?.profit_factor ?? row.test_metrics?.profit_factor ?? row.evidence?.test_profit_factor ?? 0) > 1 ? 'pos' : 'neg'">{{ row.pf_after_test ?? row.test_metrics?.after?.profit_factor ?? row.test_metrics?.profit_factor ?? row.evidence?.test_profit_factor ?? '-' }}</td>
                    <td>{{ formatPct(row.trade_coverage_test ?? row.test_metrics?.kept_coverage ?? row.coverage ?? row.evidence?.coverage) }}</td>
                    <td>{{ row.overfit_risk || row.evidence?.overfit_risk || '-' }}</td>
                    <td><small>{{ JSON.stringify(row.config_patch_preview || row.config_patch || {}) }}</small></td>
                  </tr>
                  <tr v-if="!(backtestLabGateCandidates.candidates || backtestLabGateRecommendations.recommendations || []).length"><td colspan="9" class="empty-state">No shadow candidates yet. Run Build Candidates after TQ materialization.</td></tr>
                </tbody></table>
              </div>
              <div class="table-scroll">
                <h3>Score Stability</h3>
                <table><thead><tr><th>Feature</th><th>Bucket</th><th>Samples</th><th>Train PF</th><th>Validation PF</th><th>Test PF</th><th>Monotonic</th><th>Risk</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabGateScores.scores || []).slice(0, 30)" :key="row.validation_id || row.score_id">
                    <td>{{ row.score_name || row.feature_name }}</td><td>{{ row.best_cutoff || row.bucket_key }}</td><td>{{ row.bucket_count ?? row.sample_count }}</td><td>{{ row.metrics?.splits?.train?.profit_factor ?? row.train_metrics?.profit_factor ?? '-' }}</td><td>{{ row.metrics?.splits?.validation?.profit_factor ?? row.validation_metrics?.profit_factor ?? '-' }}</td><td :class="Number(row.pf_after_test ?? row.metrics?.splits?.test?.profit_factor ?? row.test_metrics?.profit_factor ?? 0) > 1 ? 'pos' : 'neg'">{{ row.pf_after_test ?? row.metrics?.splits?.test?.profit_factor ?? row.test_metrics?.profit_factor ?? '-' }}</td><td>{{ row.train_monotonic && row.validation_monotonic && row.test_monotonic ? 'stable' : 'mixed' }}</td><td>{{ row.overfit_risk }}</td>
                  </tr>
                  <tr v-if="!(backtestLabGateScores.scores || []).length"><td colspan="8" class="empty-state">No score validation rows.</td></tr>
                </tbody></table>
              </div>
            </div>
            <div class="table-scroll">
              <h3>Bucket Diagnostics</h3>
              <table><thead><tr><th>Dimension</th><th>Bucket</th><th>Samples</th><th>Win</th><th>PF</th><th>Avg R</th><th>Cost</th><th>Target</th></tr></thead><tbody>
                <tr v-for="row in (backtestLabGateBuckets.buckets || []).slice(0, 40)" :key="row.bucket_id">
                  <td>{{ row.dimension }}</td><td>{{ row.bucket_key }}</td><td>{{ row.sample_count }}</td><td>{{ formatPct(row.metrics?.win_rate) }}</td><td :class="Number(row.metrics?.profit_factor || 0) > 1 ? 'pos' : 'neg'">{{ row.metrics?.profit_factor ?? '-' }}</td><td>{{ signedR(row.metrics?.expectancy_R ?? row.metrics?.avg_net_R) }}</td><td>{{ row.metrics?.fee_ratio ?? row.metrics?.avg_cost_bps ?? '-' }}</td><td><small>{{ row.evidence?.sample_status || row.target_hint || '-' }}</small></td>
                </tr>
                <tr v-if="!(backtestLabGateBuckets.buckets || []).length"><td colspan="8" class="empty-state">No bucket diagnostics yet.</td></tr>
              </tbody></table>
            </div>
          </div>
        </article>

        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Kline Cache Coverage</div><span class="tag blue">{{ backtestLabKlineStatus?.count || 0 }} symbols</span></div>
          <div class="panel-body table-scroll">
            <table><thead><tr><th>Symbol</th><th>Status</th><th>Rows</th><th>Coverage</th><th>First</th><th>Last</th></tr></thead><tbody>
              <tr v-for="row in (backtestLabKlineStatus?.symbols || []).slice(0, 80)" :key="row.symbol">
                <td>{{ row.symbol }}</td>
                <td><span class="tag" :class="row.status === 'ready' ? 'good' : 'warn'">{{ row.status }}</span></td>
                <td>{{ row.row_count }} / {{ row.expected_rows }}</td>
                <td>{{ formatPct(row.coverage) }}</td>
                <td>{{ row.first_open_time || "-" }}</td>
                <td>{{ row.last_open_time || "-" }}</td>
              </tr>
            </tbody></table>
          </div>
        </article>

        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Recent Experiments</div><span class="tag blue">{{ backtestLabExperiments.count || 0 }} rows</span></div>
            <div class="panel-body notice-list">
              <div v-for="row in (backtestLabExperiments.experiments || []).slice(0, 8)" :key="row.experiment_id" class="notice">
                <span><button class="link-btn" @click="openBacktestLabExperiment(row.experiment_id)"><strong>{{ row.experiment_id }}</strong></button> {{ row.strategy_line }} · {{ row.days }}d · {{ row.parameter_set_count }} sets</span>
                <span class="tag" :class="Number(row.best_profit_factor || 0) > 1 ? 'good' : 'warn'">PF {{ row.best_profit_factor ?? "-" }}</span>
              </div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Recommendations</div><span class="tag blue">{{ backtestLabRecommendations.count || 0 }} rows</span></div>
            <div class="panel-body notice-list">
              <div v-for="row in (backtestLabRecommendations.recommendations || []).slice(0, 8)" :key="row.recommendation_id" class="notice">
                <span><strong>{{ row.status }}</strong> {{ row.summary }}</span>
                <span class="tag" :class="row.status === 'candidate_pf_gt_1' ? 'good' : 'blue'">{{ row.parameter_set_id }}</span>
              </div>
            </div>
          </article>
        </div>

        <article v-if="backtestLabSelectedExperiment" class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Experiment Drilldown</div>
            <span class="tag blue">{{ backtestLabSelectedExperiment }}</span>
          </div>
          <div class="panel-body">
            <div class="wide-grid">
              <div class="table-scroll">
                <h3>Daily Metrics</h3>
                <table><thead><tr><th>Day</th><th>Line</th><th>Parameter</th><th>PF</th><th>Total R</th><th>Trades</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabExperimentDaily.rows || []).slice(0, 80)" :key="row.metric_id">
                    <td>{{ row.day }}</td><td>{{ row.strategy_line }}</td><td>{{ row.parameter_set_id }}</td><td>{{ row.metrics?.profit_factor ?? "-" }}</td><td>{{ signedR(row.metrics?.total_R) }}</td><td>{{ row.metrics?.trade_count }}</td>
                  </tr>
                </tbody></table>
              </div>
              <div class="table-scroll">
                <h3>Symbol Metrics</h3>
                <table><thead><tr><th>Symbol</th><th>Line</th><th>PF</th><th>Total R</th><th>Trades</th><th>Win</th></tr></thead><tbody>
                  <tr v-for="row in (backtestLabExperimentSymbols.rows || []).slice(0, 80)" :key="row.metric_id">
                    <td>{{ row.symbol }}</td><td>{{ row.strategy_line }}</td><td>{{ row.metrics?.profit_factor ?? "-" }}</td><td>{{ signedR(row.metrics?.total_R) }}</td><td>{{ row.metrics?.trade_count }}</td><td>{{ formatPct(row.metrics?.win_rate) }}</td>
                  </tr>
                </tbody></table>
              </div>
            </div>
            <div class="table-scroll">
              <h3>Shadow Orders</h3>
              <table><thead><tr><th>Symbol</th><th>Line</th><th>Side</th><th>Entry</th><th>Exit</th><th>Net R</th><th>Reason</th><th>Evaluator</th></tr></thead><tbody>
                <tr v-for="row in (backtestLabExperimentOrders.orders || []).slice(0, 80)" :key="row.order_id">
                  <td>{{ row.symbol }}</td><td>{{ row.strategy_line }}</td><td>{{ row.side }}</td><td>{{ price(row.entry_price) }}</td><td>{{ row.exit_reason || "-" }}</td><td :class="pnlClass(row.net_R)">{{ signedR(row.net_R) }}</td><td>{{ row.reasons?.join(", ") || "-" }}</td><td>{{ row.lineage_mode || "-" }}</td>
                </tr>
              </tbody></table>
            </div>
          </div>
        </article>
      </section>

      <section v-else-if="activePage === 'sandbox-lab'">
        <PageHeader title="Strategy Sandbox Lab" subtitle="Independent sandbox.db, manifest, backtest/replay/TQ/gate/paper-shadow, and LLM dataset export context." />

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Sandbox Plan Selector</div>
            <div class="title-actions">
              <span class="tag blue">{{ sandboxLabList.count || 0 }} sandboxes</span>
              <button class="btn small" :disabled="sandboxLabLoading" @click="loadSandboxLabPage"><RefreshCw />Refresh</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="form-grid">
              <label>Strategy
                <select v-model="sandboxLabFilters.strategy_line" @change="loadSandboxLabPage">
                  <option value="all">all</option>
                  <option value="strategy5">strategy5</option>
                  <option value="strategy6">strategy6</option>
                  <option value="without_micro">without_micro</option>
                  <option value="strategy4">strategy4</option>
                </select>
              </label>
              <label>Status
                <select v-model="sandboxLabFilters.status" @change="loadSandboxLabPage">
                  <option value="all">all</option>
                  <option value="created">created</option>
                  <option value="completed">completed</option>
                  <option value="archived">archived</option>
                </select>
              </label>
              <label>Tag<input v-model="sandboxLabFilters.tag" placeholder="smoke, review" /></label>
              <label>Limit<input v-model.number="sandboxLabFilters.limit" type="number" min="1" max="500" /></label>
            </div>
            <div class="table-scroll">
              <table>
                <thead><tr><th>Topology</th><th>Sandbox</th><th>Branches</th><th>Status</th><th>Best PF</th><th>Last Job</th><th>Updated</th><th>Action</th></tr></thead>
                <tbody>
                  <tr v-for="item in sandboxLabRows" :key="item.sandbox_id">
                    <td>{{ item.strategy_line }}</td>
                    <td class="path">{{ item.sandbox_id }}</td>
                    <td>{{ (item.branches_summary?.strategy_lines || [item.legacy_strategy_line]).filter(Boolean).join(" / ") || "-" }}</td>
                    <td><span class="tag blue">{{ item.status }}</span></td>
                    <td>{{ formatNumber(item.best_pf, 3) }}</td>
                    <td>{{ item.last_job_status || "-" }}</td>
                    <td class="path">{{ item.updated_at || item.created_at }}</td>
                    <td><button class="btn tiny" :disabled="sandboxLabLoading" @click="selectSandboxPlan(item.sandbox_id)">Select</button></td>
                  </tr>
                  <tr v-if="!sandboxLabRows.length"><td colspan="8" class="empty-state">No sandbox plan yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </article>

        <div class="dashboard-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Create Sandbox</div></div>
            <div class="panel-body">
              <div class="form-grid">
                <label>Topology
                  <select v-model="sandboxLabCreateDraft.strategy_line">
                  <option value="experiment">experiment sandbox</option>
                    <option value="micro_full">legacy strategy3 / micro_full sandbox</option>
                    <option value="micro_fast">legacy strategy2 / micro_fast sandbox</option>
                    <option value="strategy6">legacy strategy6 sandbox</option>
                    <option value="strategy5">legacy strategy5 sandbox</option>
                    <option value="strategy4">legacy strategy4 sandbox</option>
                    <option value="without_micro">legacy strategy1 / without_micro sandbox</option>
                  </select>
                </label>
                <label>Version<input v-model="sandboxLabCreateDraft.strategy_version" /></label>
                <label>Tags<input v-model="sandboxLabCreateDraft.tags" /></label>
              </div>
              <div class="action-row wrap" v-if="sandboxLabCreateDraft.strategy_line === 'experiment'">
                <label class="check-row"><input type="checkbox" value="without_micro" v-model="sandboxLabCreateDraft.strategy_lines" />strategy1 / without_micro</label>
                <label class="check-row"><input type="checkbox" value="micro_fast" v-model="sandboxLabCreateDraft.strategy_lines" />strategy2 / micro_fast</label>
                <label class="check-row"><input type="checkbox" value="micro_full" v-model="sandboxLabCreateDraft.strategy_lines" />strategy3 / micro_full</label>
                <label class="check-row"><input type="checkbox" value="strategy4" v-model="sandboxLabCreateDraft.strategy_lines" />strategy4</label>
                <label class="check-row"><input type="checkbox" value="strategy5" v-model="sandboxLabCreateDraft.strategy_lines" />strategy5</label>
                <label class="check-row"><input type="checkbox" value="strategy6" v-model="sandboxLabCreateDraft.strategy_lines" />strategy6</label>
              </div>
              <label class="check-row"><input type="checkbox" v-model="sandboxLabCreateDraft.set_active_after_create" />Set active after create</label>
              <label>Data Scope JSON<textarea v-model="sandboxLabCreateDraft.data_scope" class="config-textarea compact"></textarea></label>
              <label>Config Scope JSON<textarea v-model="sandboxLabCreateDraft.config_scope" class="config-textarea compact"></textarea></label>
              <button class="btn primary" :disabled="sandboxLabLoading" @click="createSandboxPlan"><DatabaseZap />Create Sandbox</button>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Active Summary</div>
              <span class="tag blue">{{ sandboxLabActive?.active?.strategy_line || "no sandbox" }}</span>
            </div>
            <div class="panel-body">
              <div class="stat-cards">
                <div class="stat"><span>Sandbox</span><strong>{{ sandboxLabSelectedId || "-" }}</strong><small>analysis context only</small></div>
                <div class="stat"><span>Best PF</span><strong>{{ formatNumber(sandboxLabSummary?.best_pf, 3) }}</strong><small>{{ sandboxLabSummary?.best_candidate_id || "no candidate" }}</small></div>
                <div class="stat"><span>Storage</span><strong>{{ bytesLabel(sandboxLabSummary?.storage_bytes || 0) }}</strong><small>isolated directory</small></div>
                <div class="stat"><span>Baseline</span><strong>{{ sandboxLabSelected?.baseline_context_id || "-" }}</strong><small>{{ sandboxLabSelected?.reset_mode || "reset_from_main_baseline" }}</small></div>
                <div class="stat"><span>Write Scope</span><strong>{{ sandboxLabSelected?.write_scope || "-" }}</strong><small>{{ sandboxLabSelected?.write_guard_status || "guarded" }}</small></div>
                <div class="stat"><span>Last Job</span><strong>{{ sandboxLabLastJobStatus || "-" }}</strong><small>{{ sandboxLabJobRunning ? "running controls locked" : "ready" }}</small></div>
              </div>
              <div class="table-scroll">
                <h3>Strategy Branches</h3>
                <table>
                  <thead><tr><th>Branch</th><th>Status</th><th>PF</th><th>Trades</th><th>TQ</th><th>Gate</th></tr></thead>
                  <tbody>
                    <tr v-for="branch in sandboxLabBranches.branches || []" :key="branch.branch_id">
                      <td>{{ branch.strategy_line }}</td>
                      <td><span class="tag blue">{{ branch.branch_status }}</span></td>
                      <td>{{ formatNumber(branch.branch_metrics?.profit_factor, 3) }}</td>
                      <td>{{ branch.branch_metrics?.trade_count || 0 }}</td>
                      <td>{{ branch.branch_metrics?.tq_sample_count || 0 }}</td>
                      <td>{{ branch.branch_metrics?.gate_candidate_count || 0 }}</td>
                    </tr>
                    <tr v-if="!(sandboxLabBranches.branches || []).length"><td colspan="6" class="empty-state">No branch manifest yet.</td></tr>
                  </tbody>
                </table>
              </div>
              <div class="table-scroll">
                <table><tbody>
                  <tr v-for="(value, key) in sandboxLabSummary?.counts || {}" :key="key"><td>{{ key }}</td><td>{{ value }}</td></tr>
                </tbody></table>
              </div>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Code Overlay / Runtime</div>
              <span class="tag" :class="sandboxLabCodeOverlay?.active_overlay ? 'good' : 'warn'">{{ sandboxLabCodeOverlay?.active_overlay ? 'overlay active' : 'baseline runtime' }}</span>
            </div>
            <div class="panel-body">
              <div class="form-grid">
                <label>Branch
                  <select v-model="sandboxLabSelectedBranch" @change="changeSandboxBranchForOverlay">
                    <option v-for="line in sandboxBranchLines()" :key="line" :value="line">{{ line }}</option>
                  </select>
                </label>
                <label>Target Relpath<input v-model="sandboxLabPatchDraft.target_relpath" placeholder="notes/strategy-experiment.md" /></label>
                <label>Patch Note<input v-model="sandboxLabPatchDraft.note" placeholder="sandbox-only note" /></label>
              </div>
              <label>Diff Preview<textarea v-model="sandboxLabPatchDraft.diff_text" class="config-textarea compact" placeholder="sandbox patch diff or design note"></textarea></label>
              <div class="action-row wrap">
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="createSandboxCodeOverlay">Create Overlay</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="addSandboxCodePatch">Add Patch</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="buildSandboxRuntime">Build Runtime</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="smokeSandboxRuntime">Runtime Smoke</button>
              </div>
              <div class="stat-cards">
                <div class="stat"><span>Overlay</span><strong>{{ sandboxLabCodeOverlay?.overlay_count || 0 }}</strong><small>{{ sandboxLabCodeOverlay?.active_overlay?.code_overlay_id || "baseline only" }}</small></div>
                <div class="stat"><span>Patches</span><strong>{{ sandboxLabCodeOverlay?.patch_count || 0 }}</strong><small>sandbox overlay only</small></div>
                <div class="stat"><span>Runtime</span><strong>{{ sandboxLabCodeOverlay?.runtime_count || 0 }}</strong><small>{{ sandboxLabCodeOverlay?.runtimes?.[0]?.status || "not built" }}</small></div>
              </div>
              <div class="table-scroll compact">
                <table>
                  <thead><tr><th>Patch</th><th>Target</th><th>Status</th><th>Created</th></tr></thead>
                  <tbody>
                    <tr v-for="patch in (sandboxLabCodeOverlay?.patches || []).slice(0, 5)" :key="patch.code_patch_id">
                      <td class="path">{{ patch.code_patch_id }}</td>
                      <td class="path">{{ patch.target_relpath }}</td>
                      <td><span class="tag blue">{{ patch.status }}</span></td>
                      <td class="path">{{ patch.created_at }}</td>
                    </tr>
                    <tr v-if="!(sandboxLabCodeOverlay?.patches || []).length"><td colspan="4" class="empty-state">No sandbox code patch yet.</td></tr>
                  </tbody>
                </table>
              </div>
              <small class="muted">Sandbox code overlay only affects the selected sandbox branch. Promotion to baseline requires a separate task.</small>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header">
              <div class="panel-title"><span class="accent"></span>Contract Jobs</div>
              <span class="tag" :class="sandboxLabHealth?.integrity_check === 'ok' ? 'good' : 'warn'">{{ sandboxLabHealth?.integrity_check || "unchecked" }}</span>
            </div>
            <div class="panel-body">
              <div class="action-row wrap">
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('backtest')">Backtest</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('replay')">Replay</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('trade-quality')">TQ</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('gate-search')">Gate</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('holdout')">Holdout</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('config-export')">Config</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('paper-shadow')">Paper Shadow</button>
                <button class="btn tiny" :disabled="sandboxLabActionDisabled" @click="runSandboxJob('llm-export')">LLM Export</button>
                <button class="btn tiny danger" :disabled="sandboxLabActionDisabled" @click="deleteSandboxPlan('soft_delete')">Soft Delete</button>
              </div>
              <div v-if="sandboxLabJobRunning" class="alert warn">Sandbox job is running or queued; mutation controls are locked until the status refreshes.</div>
              <div v-if="sandboxLabMessage" class="alert info">{{ sandboxLabMessage }}</div>
              <pre>{{ JSON.stringify(sandboxLabLastJob || sandboxLabHealth || {}, null, 2) }}</pre>
            </div>
          </article>
        </div>

        <div class="dashboard-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Branch Leaderboard</div><span class="tag blue">{{ sandboxLabLeaderboard.count || 0 }} branches</span></div>
            <div class="panel-body table-scroll">
              <table>
                <thead><tr><th>Branch</th><th>PF</th><th>Trades</th><th>TQ</th><th>Gate</th><th>Best Candidate</th></tr></thead>
                <tbody>
                  <tr v-for="row in sandboxLabLeaderboard.leaderboard || []" :key="row.branch_id">
                    <td>{{ row.strategy_line }}</td><td>{{ formatNumber(row.best_pf, 3) }}</td><td>{{ row.trade_count }}</td><td>{{ row.tq_sample_count }}</td><td>{{ row.gate_candidate_count }}</td><td class="path">{{ row.best_candidate_id || "-" }}</td>
                  </tr>
                  <tr v-if="!(sandboxLabLeaderboard.leaderboard || []).length"><td colspan="6" class="empty-state">No leaderboard rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </article>

          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>TQ / Gate Compare</div><span class="tag blue">{{ sandboxLabTqCompare.count || 0 }} TQ buckets</span></div>
            <div class="panel-body table-scroll">
              <table>
                <thead><tr><th>Branch</th><th>Root Cause</th><th>Samples</th><th>WR</th><th>Avg R</th><th>MFE</th><th>MAE</th></tr></thead>
                <tbody>
                  <tr v-for="row in (sandboxLabTqCompare.items || []).slice(0, 20)" :key="`${row.strategy_line}-${row.root_cause}`">
                    <td>{{ row.strategy_line }}</td><td>{{ row.root_cause }}</td><td>{{ row.sample_count }}</td><td>{{ formatPct(row.win_rate) }}</td><td>{{ signedR(row.avg_R) }}</td><td>{{ signedR(row.avg_MFE_R) }}</td><td>{{ signedR(row.avg_MAE_R) }}</td>
                  </tr>
                  <tr v-if="!(sandboxLabTqCompare.items || []).length"><td colspan="7" class="empty-state">No Trade Quality compare rows yet.</td></tr>
                </tbody>
              </table>
              <table>
                <thead><tr><th>Branch</th><th>Candidate</th><th>PF</th><th>Status</th><th>Risk</th></tr></thead>
                <tbody>
                  <tr v-for="row in (sandboxLabGateCompare.items || []).slice(0, 10)" :key="row.candidate_id">
                    <td>{{ row.strategy_line }}</td><td class="path">{{ row.candidate_id }}</td><td>{{ formatNumber(row.test_metrics?.profit_factor, 3) }}</td><td>{{ row.status }}</td><td>{{ row.overfit_risk }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </article>
        </div>
      </section>

      <section v-else-if="activePage === 'research-db'">
        <PageHeader title="Unified Research DB" subtitle="Baseline trade fact, entry-known features, Trade Quality targets, and dataset cards for research and LLM training." />

        <article class="card panel">
          <div class="panel-header">
            <div class="panel-title"><span class="accent"></span>Research DB Controls</div>
            <div class="title-actions">
              <span class="tag blue">{{ researchDb.summary?.schema_version || "research_db_v1" }}</span>
              <button class="btn small" :disabled="researchDbLoading" @click="loadResearchDbPage"><RefreshCw />Refresh</button>
              <button class="btn small" :disabled="researchDbLoading" @click="materializeResearchDb(true)">Dry Run</button>
              <button class="btn cycle" :disabled="researchDbLoading" @click="materializeResearchDb(false)"><DatabaseZap />Materialize</button>
            </div>
          </div>
          <div class="panel-body">
            <div class="form-grid">
              <label>Strategy
                <select v-model="researchDbFilters.strategy_line" @change="loadResearchDbPage">
                  <option value="all">all</option>
                  <option value="without_micro">without_micro</option>
                  <option value="micro_fast">micro_fast</option>
                  <option value="micro_full">micro_full</option>
                  <option value="strategy4">strategy4</option>
                  <option value="strategy5">strategy5</option>
                  <option value="strategy6">strategy6</option>
                </select>
              </label>
              <label>Source
                <select v-model="researchDbFilters.source_type" @change="loadResearchDbPage">
                  <option value="all">all</option>
                  <option value="backtest">backtest</option>
                  <option value="paper">paper</option>
                  <option value="sandbox">sandbox</option>
                  <option value="archive">archive</option>
                </select>
              </label>
              <label>Rows<input v-model.number="researchDbFilters.limit" type="number" min="20" max="500" /></label>
            </div>
            <div v-if="researchDbMessage" class="alert info">{{ researchDbMessage }}</div>
            <div class="stat-cards">
              <div class="stat"><span>Trade Facts</span><strong>{{ researchDb.summary?.counts?.trade_facts || 0 }}</strong><small>backtest / paper / sandbox-ready</small></div>
              <div class="stat"><span>Entry Features</span><strong>{{ researchDb.summary?.counts?.entry_features || 0 }}</strong><small>entry-known only</small></div>
              <div class="stat"><span>TQ Samples</span><strong>{{ researchDb.summary?.counts?.tq_samples || 0 }}</strong><small>targets separated</small></div>
              <div class="stat"><span>Feature Completeness</span><strong>{{ formatPct(researchDb.summary?.feature_quality?.avg_feature_completeness) }}</strong><small>{{ researchDb.summary?.feature_quality?.proxy_rows || 0 }} proxy rows</small></div>
            </div>
            <div class="stat-cards">
              <div class="stat"><span>Writer Contract</span><strong>{{ (researchDb.writerStatus?.writers || []).length }}</strong><small>active/materialized writer rows</small></div>
              <div class="stat"><span>Fact Coverage</span><strong>{{ researchDb.fieldCoverage?.fact_count || 0 }}</strong><small>{{ formatPct(researchDb.fieldCoverage?.feature_coverage) }} feature / {{ formatPct(researchDb.fieldCoverage?.tq_coverage) }} TQ</small></div>
              <div class="stat"><span>Lineage Audit</span><strong :class="researchDb.lineageAudit?.status === 'ok' ? 'pos' : 'neg'">{{ researchDb.lineageAudit?.status || 'unknown' }}</strong><small>{{ researchDb.lineageAudit?.missing_lineage_rows || 0 }} missing lineage</small></div>
              <div class="stat"><span>No-Lookahead</span><strong :class="Number(researchDb.lineageAudit?.known_after_entry_rows || 0) === 0 ? 'pos' : 'neg'">{{ researchDb.lineageAudit?.known_after_entry_rows || 0 }}</strong><small>known_at after entry rows</small></div>
            </div>
          </div>
        </article>

        <div class="dashboard-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>P24 Writer Status</div><span class="tag blue">{{ researchDb.writerStatus?.writers?.length || 0 }} writers</span></div>
            <div class="panel-body table-scroll">
              <table>
                <thead><tr><th>Source</th><th>Strategy</th><th>Facts</th><th>Lineage Rows</th><th>Status</th></tr></thead>
                <tbody>
                  <tr v-for="row in researchDb.writerStatus?.writers || []" :key="`${row.source_type}-${row.strategy_line}`">
                    <td>{{ row.source_type }}</td><td>{{ row.strategy_line }}</td><td>{{ row.facts }}</td><td>{{ row.lineage_rows }}</td><td><span class="tag good">{{ row.status }}</span></td>
                  </tr>
                  <tr v-if="!(researchDb.writerStatus?.writers || []).length"><td colspan="5" class="empty-state">No writer status rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Missing / Proxy Fields</div><span class="tag" :class="researchDb.lineageAudit?.status === 'ok' ? 'good' : 'warn'">{{ researchDb.lineageAudit?.status || 'unknown' }}</span></div>
            <div class="panel-body">
              <div class="wide-grid">
                <div class="table-scroll compact">
                  <h3>Missing</h3>
                  <table><thead><tr><th>Field</th><th>Count</th></tr></thead><tbody>
                    <tr v-for="row in researchDb.fieldCoverage?.missing_fields_top || []" :key="row.field"><td>{{ row.field }}</td><td>{{ row.count }}</td></tr>
                    <tr v-if="!(researchDb.fieldCoverage?.missing_fields_top || []).length"><td colspan="2" class="empty-state">No missing fields.</td></tr>
                  </tbody></table>
                </div>
                <div class="table-scroll compact">
                  <h3>Proxy</h3>
                  <table><thead><tr><th>Field</th><th>Count</th></tr></thead><tbody>
                    <tr v-for="row in researchDb.fieldCoverage?.proxy_fields_top || []" :key="row.field"><td>{{ row.field }}</td><td>{{ row.count }}</td></tr>
                    <tr v-if="!(researchDb.fieldCoverage?.proxy_fields_top || []).length"><td colspan="2" class="empty-state">No proxy fields.</td></tr>
                  </tbody></table>
                </div>
              </div>
            </div>
          </article>
        </div>

        <div class="dashboard-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Strategy Coverage</div><span class="tag blue">{{ researchDb.summary?.strategies?.length || 0 }} lines</span></div>
            <div class="panel-body table-scroll">
              <table>
                <thead><tr><th>Strategy</th><th>Trade Facts</th></tr></thead>
                <tbody>
                  <tr v-for="row in researchDb.summary?.strategies || []" :key="row.strategy_line">
                    <td>{{ row.strategy_line }}</td><td>{{ row.trade_facts }}</td>
                  </tr>
                  <tr v-if="!(researchDb.summary?.strategies || []).length"><td colspan="2" class="empty-state">No research DB rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Dataset Card</div><span class="tag blue">{{ researchDb.datasetCards?.cards?.length || 0 }} cards</span></div>
            <div class="panel-body">
              <pre>{{ JSON.stringify((researchDb.datasetCards?.cards || [])[0] || researchDb.summary?.latest_dataset_card || {}, null, 2) }}</pre>
            </div>
          </article>
        </div>

        <article class="card panel">
          <div class="panel-header"><div class="panel-title"><span class="accent"></span>Trade Fact Ledger</div><span class="tag blue">{{ researchDb.tradeFacts?.total || 0 }} rows</span></div>
          <div class="panel-body table-scroll">
            <table>
              <thead><tr><th>Sample</th><th>Source</th><th>Strategy</th><th>Symbol</th><th>Side</th><th>Net R</th><th>Entry</th><th>Exit</th><th>Field Quality</th></tr></thead>
              <tbody>
                <tr v-for="row in researchDb.tradeFacts?.rows || []" :key="row.sample_id">
                  <td class="path">{{ row.sample_id }}</td><td>{{ row.source_type }}</td><td>{{ row.strategy_line }}</td><td>{{ row.symbol }}</td><td>{{ row.side }}</td><td :class="pnlClass(row.net_R)">{{ signedR(row.net_R) }}</td><td>{{ price(row.entry_price) }}</td><td>{{ price(row.exit_price) }}</td><td class="path">{{ row.field_quality_json }}</td>
                </tr>
                <tr v-if="!(researchDb.tradeFacts?.rows || []).length"><td colspan="9" class="empty-state">No trade facts yet.</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Entry-Known Features</div><span class="tag blue">{{ researchDb.entryFeatures?.total || 0 }} rows</span></div>
            <div class="panel-body table-scroll">
              <table>
                <thead><tr><th>Feature Sample</th><th>Strategy</th><th>Symbol</th><th>Completeness</th><th>Proxy</th><th>Missing</th><th>Source</th></tr></thead>
                <tbody>
                  <tr v-for="row in researchDb.entryFeatures?.rows || []" :key="row.feature_sample_id">
                    <td class="path">{{ row.feature_sample_id }}</td><td>{{ row.strategy_line }}</td><td>{{ row.symbol }}</td><td>{{ formatPct(row.feature_completeness) }}</td><td>{{ row.proxy_level }}</td><td class="path">{{ row.missing_fields_json }}</td><td>{{ row.source_level }}</td>
                  </tr>
                  <tr v-if="!(researchDb.entryFeatures?.rows || []).length"><td colspan="7" class="empty-state">No entry feature rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Trade Quality Research Samples</div><span class="tag blue">{{ researchDb.tqSamples?.total || 0 }} rows</span></div>
            <div class="panel-body table-scroll">
              <table>
                <thead><tr><th>Sample</th><th>Strategy</th><th>Symbol</th><th>Net R</th><th>MFE</th><th>MAE</th><th>Root</th><th>Deep</th></tr></thead>
                <tbody>
                  <tr v-for="row in researchDb.tqSamples?.rows || []" :key="row.research_tq_id">
                    <td class="path">{{ row.sample_id }}</td><td>{{ row.strategy_line }}</td><td>{{ row.symbol }}</td><td :class="pnlClass(row.net_R)">{{ signedR(row.net_R) }}</td><td>{{ signedR(row.MFE_R) }}</td><td>{{ signedR(row.MAE_R) }}</td><td>{{ row.root_cause || "-" }}</td><td>{{ row.deep_subcause || "-" }}</td>
                  </tr>
                  <tr v-if="!(researchDb.tqSamples?.rows || []).length"><td colspan="8" class="empty-state">No TQ research rows yet.</td></tr>
                </tbody>
              </table>
            </div>
          </article>
        </div>
      </section>

      <section v-else-if="activePage === 'notifications'">
        <PageHeader title="Notifications / Feishu" subtitle="P15 飞书通知配置、测试发送、投递历史和失败摘要。" />
        <div class="wide-grid">
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Feishu Config</div><span class="tag" :class="feishu.configured ? 'good' : 'warn'">{{ feishu.configured ? 'configured' : 'missing' }}</span></div>
            <div class="panel-body form-grid">
              <label>enabled<input :value="feishu.enabled"></label>
              <label>message_mode<input :value="feishu.message_mode"></label>
              <label>webhook_url<input :value="feishu.webhook_url"></label>
              <label>webhook_secret<input :value="feishu.webhook_secret"></label>
              <div class="action-row"><button class="btn primary" @click="sendFeishuMockSignals"><Send />Send Mock 3 Cards</button><button class="btn cycle" @click="sendFeishuCurrentSignals"><Bell />Send Current Signals</button><button class="btn" @click="api.feishuTest().then(refreshAll)"><Zap />Test API</button></div>
            </div>
          </article>
          <article class="card panel">
            <div class="panel-header"><div class="panel-title"><span class="accent"></span>Delivery History</div><span class="tag blue">{{ deliveries.length }} rows</span></div>
            <div class="panel-body notice-list">
              <div v-for="row in deliveries.slice().reverse().slice(0, 8)" :key="row.dedup_key + row.created_at" class="notice">
                <span><strong>{{ row.strategy_name || row.strategy_line }}</strong> {{ row.symbol }} {{ row.side }}</span>
                <span class="tag" :class="statusClass(row.status)">{{ row.status }}</span>
              </div>
            </div>
          </article>
        </div>
      </section>
    </main>

    <div v-if="detailModal" class="modal-backdrop" @click.self="detailModal = null">
      <div class="modal card">
        <div class="panel-header"><div class="panel-title"><span class="accent"></span>{{ lineLabel(detailModal.line) }} / {{ detailModal.symbol }}</div><button class="btn" @click="detailModal = null">Close</button></div>
        <div class="panel-body">
          <div v-if="detailModal.row" class="trade-detail-grid">
            <div><span>方向</span><strong>{{ sideLabel(detailModal.row.side) }}</strong></div>
            <div><span>订单类型</span><strong>{{ orderTypeLabel(detailModal.row.order_type || 'market') }}</strong></div>
            <div><span>开仓价</span><strong>{{ price(detailModal.row.filled_entry_price || detailModal.row.entry_price) }}</strong></div>
            <div><span>止损 / 止盈</span><strong>{{ price(detailModal.row.stop_loss) }} / {{ price(detailModal.row.take_profit) }}</strong></div>
            <div><span>Risk Budget</span><strong>{{ money(detailModal.row.risk_budget_usdt || detailModal.row.estimated_max_loss_usdt) }}U</strong></div>
            <div><span>Notional / Margin</span><strong>{{ money(detailModal.row.planned_notional_usdt || detailModal.row.notional_usdt) }} / {{ money(detailModal.row.margin_usdt) }}U</strong></div>
            <div><span>盈亏</span><strong :class="pnlClass(detailModal.row.realized_pnl_usdt || detailModal.row.unrealized_pnl_usdt)">{{ money(detailModal.row.realized_pnl_usdt || detailModal.row.unrealized_pnl_usdt) }}</strong></div>
            <div><span>平仓原因</span><strong>{{ exitReasonLabel(detailModal.row.exit_reason) }}</strong></div>
          </div>
          <div class="chart-box">
            <div v-for="(candle, idx) in detailModal.payload?.candles?.slice?.(-80) || []" :key="idx" class="candle" :class="Number(candle.close) >= Number(candle.open) ? 'up-candle' : 'down-candle'" :style="{ height: `${Math.max(8, Math.min(90, Math.abs(Number(candle.close) - Number(candle.open)) * 8000))}px` }"></div>
          </div>
          <div class="wide-grid">
            <div class="level-list">
              <div v-for="level in detailModal.payload?.levels || []" :key="level.type" class="notice"><span>{{ level.label }}</span><strong>{{ price(level.price) }}</strong></div>
            </div>
            <div class="level-list">
              <div v-for="marker in detailModal.payload?.markers || []" :key="marker.fill_id" class="notice"><span>{{ marker.label }}</span><strong>{{ price(marker.price) }}</strong></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style>
:root {
  --bg: #071016;
  --bg-2: #0a151d;
  --surface: rgba(13, 27, 37, 0.92);
  --surface-2: rgba(10, 22, 31, 0.98);
  --surface-3: rgba(18, 38, 51, 0.72);
  --ink: #e7f2f5;
  --muted: #7e94a3;
  --muted-2: #526877;
  --line: rgba(108, 140, 158, 0.18);
  --line-strong: rgba(92, 238, 207, 0.28);
  --blue: #2ea8ff;
  --cyan: #20e6d0;
  --green: #21d488;
  --amber: #f4c85f;
  --orange: #ff9f43;
  --red: #ff5f73;
  --purple: #a884ff;
  --shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
  --glow-cyan: 0 0 32px rgba(32, 230, 208, 0.18);
  --radius: 20px;
  --sidebar: 248px;
  --topbar: 70px;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", Arial, sans-serif;
}

* { box-sizing: border-box; }
* {
  scrollbar-color: rgba(32, 230, 208, 0.48) rgba(7, 18, 25, 0.72);
  scrollbar-width: thin;
}
*::-webkit-scrollbar {
  width: 10px;
  height: 10px;
}
*::-webkit-scrollbar-track {
  background: rgba(7, 18, 25, 0.72);
  border-radius: 999px;
}
*::-webkit-scrollbar-thumb {
  background: linear-gradient(180deg, rgba(32, 230, 208, 0.62), rgba(46, 168, 255, 0.42));
  border: 2px solid rgba(7, 18, 25, 0.72);
  border-radius: 999px;
  box-shadow: 0 0 14px rgba(32, 230, 208, 0.18);
}
*::-webkit-scrollbar-thumb:hover {
  background: linear-gradient(180deg, rgba(32, 230, 208, 0.82), rgba(46, 168, 255, 0.58));
}
body {
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at 18% 0%, rgba(32, 230, 208, 0.13), transparent 34%),
    radial-gradient(circle at 74% 12%, rgba(46, 168, 255, 0.12), transparent 30%),
    linear-gradient(135deg, #050a0f 0%, #071018 42%, #09131c 100%);
  font-size: 14px;
  letter-spacing: 0;
  overflow-x: hidden;
}
body::before {
  content: "";
  position: fixed;
  inset: 0;
  pointer-events: none;
  opacity: 0.27;
  background-image:
    linear-gradient(rgba(255,255,255,0.035) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,0.035) 1px, transparent 1px);
  background-size: 38px 38px;
  mask-image: linear-gradient(to bottom, rgba(0,0,0,0.85), rgba(0,0,0,0.12));
}
button, input, select { font: inherit; }
button { cursor: pointer; }
.app {
  min-height: 100vh;
  display: grid;
  grid-template-columns: var(--sidebar) minmax(0, 1fr);
  grid-template-rows: var(--topbar) 1fr;
}
.sidebar {
  grid-row: 1 / span 2;
  background: linear-gradient(180deg, rgba(5, 15, 22, 0.98), rgba(6, 18, 27, 0.94));
  color: #eaf2f5;
  border-right: 1px solid var(--line);
  box-shadow: 18px 0 55px rgba(0, 0, 0, 0.28);
  display: flex;
  flex-direction: column;
  min-height: 100vh;
  position: sticky;
  top: 0;
  z-index: 20;
}
.brand {
  height: var(--topbar);
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 0 18px;
  border-bottom: 1px solid var(--line);
}
.brand-mark {
  width: 42px;
  height: 42px;
  display: grid;
  place-items: center;
  background: linear-gradient(135deg, var(--cyan), var(--blue));
  color: #061216;
  border-radius: 13px;
  font-weight: 900;
  box-shadow: 0 0 24px rgba(32, 230, 208, 0.38);
}
.brand-title { display: flex; flex-direction: column; min-width: 0; }
.brand-title strong { font-size: 15px; font-weight: 900; white-space: nowrap; }
.brand-title span { color: var(--muted); font-size: 11px; }
.nav { padding: 14px 10px; display: flex; flex-direction: column; gap: 4px; }
.nav-group-label {
  margin: 18px 10px 8px;
  color: var(--muted-2);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}
.nav button {
  border: 0;
  width: 100%;
  min-height: 42px;
  padding: 0 12px;
  color: #a9bdc8;
  background: transparent;
  border-radius: 12px;
  display: flex;
  align-items: center;
  gap: 10px;
  text-align: left;
}
.nav button:hover { background: rgba(32, 230, 208, 0.08); color: #fff; }
.nav button.active {
  color: var(--ink);
  border: 1px solid rgba(32, 230, 208, 0.24);
  background: linear-gradient(90deg, rgba(32, 230, 208, 0.18), rgba(46, 168, 255, 0.07));
  box-shadow: inset 3px 0 0 var(--cyan), var(--glow-cyan);
}
.nav svg { width: 18px; height: 18px; flex: none; }
.sidebar-footer {
  margin: auto 16px 18px;
  padding: 14px;
  border: 1px solid var(--line);
  border-radius: 16px;
  background: rgba(12, 26, 36, 0.72);
  color: var(--muted);
  font-size: 12px;
  line-height: 1.6;
}
.bot-status { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
.pulse-dot {
  width: 9px;
  height: 9px;
  border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 18px rgba(33, 212, 136, 0.9);
  animation: pulse 1.5s infinite;
  flex: none;
}
.pulse-dot.inline { display: inline-block; margin-right: 10px; transform: translateY(-3px); }
@keyframes pulse {
  0%, 100% { transform: scale(0.9); opacity: 0.75; }
  50% { transform: scale(1.25); opacity: 1; }
}
.mini-metric { display: grid; grid-template-columns: 1fr auto; gap: 7px; font-size: 11px; color: var(--muted); }
.mini-metric strong { color: var(--ink); }
.topbar {
  grid-column: 2;
  background: rgba(7, 16, 22, 0.74);
  backdrop-filter: blur(18px);
  border-bottom: 1px solid var(--line);
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 30px;
  position: sticky;
  top: 0;
  z-index: 10;
}
.topbar-left, .topbar-right { display: flex; align-items: center; gap: 14px; min-width: 0; }
.topbar h1 { font-size: 18px; margin: 0; white-space: nowrap; font-weight: 900; letter-spacing: -0.02em; }
.identity { display: flex; align-items: center; gap: 8px; color: var(--muted); font-size: 12px; min-width: 0; }
.content { grid-column: 2; padding: 26px 30px 36px; width: 100%; max-width: none; min-width: 0; position: relative; z-index: 1; }
.hero { display: grid; grid-template-columns: 1fr auto; gap: 20px; align-items: end; margin-bottom: 22px; }
.hero.compact { grid-template-columns: 1fr; }
.hero h2 { margin: 0 0 8px; font-size: 28px; line-height: 1.1; letter-spacing: -0.045em; }
.hero p { margin: 0; color: var(--muted); font-size: 13px; max-width: 860px; }
.market-strip { display: flex; align-items: center; gap: 8px; padding: 8px; border: 1px solid var(--line); border-radius: 16px; background: rgba(7, 18, 25, 0.68); }
.ticker { min-width: 102px; padding: 8px 10px; border-radius: 12px; background: rgba(15, 31, 42, 0.8); border: 1px solid rgba(255,255,255,0.05); }
.ticker .sym { display: flex; justify-content: space-between; color: var(--muted); font-size: 10px; font-weight: 800; }
.ticker .px { margin-top: 3px; font-size: 14px; font-weight: 900; }
.up { color: var(--green); }
.down { color: var(--red); }
.flat { color: var(--amber); }
.grid-kpi { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin-bottom: 14px; }
.card {
  position: relative;
  min-width: 0;
  border: 1px solid var(--line);
  border-radius: var(--radius);
  background: linear-gradient(180deg, rgba(255,255,255,0.035), transparent 80%), var(--surface);
  box-shadow: var(--shadow);
  overflow: hidden;
}
.card::after {
  content: "";
  position: absolute;
  inset: 0;
  pointer-events: none;
  border-radius: inherit;
  background: linear-gradient(135deg, rgba(32,230,208,0.08), transparent 34%, rgba(46,168,255,0.05));
  opacity: 0.8;
}
.card > * { position: relative; z-index: 1; }
.kpi { min-height: 118px; padding: 18px 18px 16px; }
.kpi-top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; }
.kpi-label { color: var(--muted); font-size: 11px; font-weight: 850; letter-spacing: 0.07em; text-transform: uppercase; }
.status-icon { width: 34px; height: 34px; display: grid; place-items: center; border-radius: 12px; background: rgba(32, 230, 208, 0.1); border: 1px solid rgba(32, 230, 208, 0.18); color: var(--cyan); }
.status-icon svg { width: 18px; height: 18px; }
.kpi-value { font-size: 28px; line-height: 1; font-weight: 950; letter-spacing: -0.055em; }
.status-value { display: flex; align-items: center; }
.kpi-sub { margin-top: 8px; color: var(--muted); font-size: 12px; }
.kpi-line { position: absolute; left: 0; right: 0; bottom: 0; height: 3px; background: linear-gradient(90deg, var(--cyan), transparent); opacity: 0.8; }
.kpi-line.amber { background: linear-gradient(90deg,var(--amber),transparent); }
.kpi-line.orange { background: linear-gradient(90deg,var(--orange),transparent); }
.dashboard-grid { display: grid; grid-template-columns: 1.1fr 1.1fr 1fr; gap: 14px; margin-bottom: 14px; }
.wide-grid { display: grid; grid-template-columns: 1.25fr 1fr; gap: 14px; margin-bottom: 14px; }
.bottom-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; }
.line-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 14px; }
.content section > .card.panel,
.content section > .dashboard-grid,
.content section > .wide-grid,
.content section > .bottom-grid,
.content section > .line-grid,
.content section > .paper-grid,
.content section > .grid-kpi {
  margin-bottom: 18px;
}
.content section > .card.panel:last-child,
.content section > .dashboard-grid:last-child,
.content section > .wide-grid:last-child,
.content section > .bottom-grid:last-child,
.content section > .line-grid:last-child,
.content section > .paper-grid:last-child,
.content section > .grid-kpi:last-child {
  margin-bottom: 0;
}
.trade-line-tabs { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 10px; }
.trade-line-tab {
  min-height: 92px;
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 12px;
  background: rgba(7, 18, 25, 0.76);
  color: var(--ink);
  text-align: left;
  display: grid;
  gap: 8px;
}
.trade-line-tab.active {
  border-color: rgba(32, 230, 208, 0.62);
  background: linear-gradient(180deg, rgba(32, 230, 208, 0.13), rgba(7, 18, 25, 0.78));
  box-shadow: inset 0 0 0 1px rgba(32, 230, 208, 0.1), 0 0 24px rgba(32, 230, 208, 0.1);
}
.trade-line-tab.skipped { opacity: 0.62; }
.trade-line-name { font-size: 13px; font-weight: 950; overflow-wrap: anywhere; }
.trade-line-meta { color: var(--muted); font-size: 11px; overflow-wrap: anywhere; }
.funnel-bar { display: grid; grid-template-columns: repeat(auto-fit, minmax(125px, 1fr)); gap: 10px; margin-bottom: 14px; }
.funnel-step {
  min-height: 72px;
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 12px;
  background: rgba(7, 18, 25, 0.7);
  display: grid;
  gap: 7px;
}
.funnel-step span { color: var(--muted); font-size: 10px; font-weight: 900; text-transform: uppercase; }
.funnel-step strong { font-size: 25px; line-height: 1; letter-spacing: -0.045em; }
.compact-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); margin-top: 10px; }
.reason-table { max-height: 420px; }
.trade-plan-symbol-table { max-height: 520px; }
.debug-details { min-height: auto; margin-top: 14px; padding: 0; }
.debug-details summary {
  min-height: 52px;
  padding: 0 16px;
  display: flex;
  align-items: center;
  font-size: 13px;
  font-weight: 900;
  cursor: pointer;
  border-bottom: 1px solid var(--line);
}
.panel { min-height: 210px; min-width: 0; }
.panel-header { min-height: 52px; display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 0 16px; border-bottom: 1px solid var(--line); }
.panel-title { display: flex; align-items: center; gap: 9px; font-size: 13px; font-weight: 900; min-width: 0; }
.title-actions { display: inline-flex; align-items: center; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
.accent { width: 8px; height: 20px; border-radius: 999px; background: linear-gradient(180deg, var(--cyan), var(--blue)); box-shadow: 0 0 18px rgba(32,230,208,0.35); }
.panel-body { padding: 16px; }
.panel-body > .muted.small:first-child {
  display: block;
  margin: 0 0 14px;
  line-height: 1.6;
}
.panel-body > .wide-grid:last-child,
.panel-body > .grid-kpi:last-child,
.panel-body > .audit-symbol-table:last-child,
.panel-body > .table-scroll:last-child {
  margin-bottom: 0;
}
.plan-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; }
.plan-box { padding: 14px; border-radius: 16px; background: rgba(7, 18, 25, 0.74); border: 1px solid var(--line); }
.plan-name { color: var(--muted); font-size: 11px; font-weight: 750; overflow-wrap: anywhere; }
.plan-num { margin-top: 8px; font-size: 30px; font-weight: 950; letter-spacing: -0.05em; }
.plan-meta { display: flex; justify-content: space-between; align-items: center; gap: 8px; margin-top: 8px; color: var(--muted); font-size: 11px; }
.chip, .tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-height: 24px;
  padding: 0 9px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 900;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.035);
  white-space: nowrap;
}
.chip { min-height: 28px; font-size: 11px; color: var(--muted); background: rgba(13, 28, 38, 0.72); }
.green, .tag.good, .chip.green { color: #adffe0; border-color: rgba(33, 212, 136, 0.28); background: rgba(33, 212, 136, 0.1); }
.yellow, .tag.warn, .chip.yellow { color: #ffe4a0; border-color: rgba(244, 200, 95, 0.3); background: rgba(244, 200, 95, 0.1); }
.tag.bad { color: #ffc1c9; border-color: rgba(255, 95, 115, 0.3); background: rgba(255, 95, 115, 0.1); }
.tag.blue, .chip.blue { color: #c6eaff; border-color: rgba(46,168,255,0.3); background: rgba(46,168,255,0.09); }
.btn {
  min-height: 38px;
  border: 1px solid var(--line);
  border-radius: 12px;
  padding: 0 14px;
  background: rgba(13, 28, 38, 0.78);
  color: var(--ink);
  font-weight: 800;
  font-size: 12px;
  letter-spacing: 0.01em;
  box-shadow: 0 12px 28px rgba(0,0,0,0.14);
  display: inline-flex;
  align-items: center;
  gap: 8px;
}
.btn svg { width: 15px; height: 15px; }
.link-btn {
  border: 0;
  background: transparent;
  color: var(--cyan);
  font: inherit;
  font-weight: 800;
  cursor: pointer;
  padding: 0;
}
.link-btn:hover { color: var(--mint); text-decoration: underline; }
.btn.primary { color: #061115; border-color: transparent; background: linear-gradient(135deg, var(--cyan), var(--blue)); box-shadow: 0 0 28px rgba(32, 230, 208, 0.26); }
.btn.cycle { color: #061115; border-color: transparent; background: linear-gradient(135deg, #21d488, #f4c85f); box-shadow: 0 0 28px rgba(33, 212, 136, 0.2); }
.btn.danger { background: linear-gradient(180deg, rgba(64,28,34,.98), rgba(40,14,19,.98)); border-color: rgba(255,107,120,.25); color: #ffd9de; }
.btn:disabled {
  cursor: not-allowed;
  opacity: 0.48;
  filter: grayscale(0.35);
  box-shadow: none;
}
.strategy-select {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 5px;
  border: 1px solid var(--line);
  border-radius: 14px;
  background: rgba(7, 18, 25, 0.72);
}
.strategy-select.compact { padding: 4px; }
.strategy-select.disabled { opacity: 0.62; }
.strategy-toggle {
  min-height: 28px;
  border: 1px solid transparent;
  border-radius: 10px;
  padding: 0 9px;
  background: transparent;
  color: var(--muted);
  font-size: 11px;
  font-weight: 900;
}
.strategy-toggle.active {
  color: #061115;
  border-color: transparent;
  background: linear-gradient(135deg, var(--cyan), var(--blue));
  box-shadow: 0 0 18px rgba(32, 230, 208, 0.16);
}
.strategy-toggle:disabled { cursor: not-allowed; }
.bars { display: grid; gap: 16px; }
.bar-row { display: grid; grid-template-columns: 74px 1fr 54px; align-items: center; gap: 10px; font-size: 12px; }
.bar-label { color: var(--muted); font-weight: 800; }
.bar-track { height: 10px; border-radius: 999px; background: rgba(126, 148, 163, 0.14); overflow: hidden; box-shadow: inset 0 0 12px rgba(0,0,0,0.35); }
.bar-fill { height: 100%; border-radius: inherit; background: linear-gradient(90deg, var(--cyan), var(--blue)); }
.bar-fill.green { background: linear-gradient(90deg, #21d488, #4dffb4); }
.bar-fill.orange { background: linear-gradient(90deg, #b77821, var(--orange)); }
.bar-value { text-align: right; font-weight: 950; }
.notice-list { display: grid; gap: 10px; }
.notice { display: grid; grid-template-columns: 1fr auto; align-items: center; gap: 10px; padding: 11px 12px; border-radius: 14px; background: rgba(7, 18, 25, 0.7); border: 1px solid var(--line); font-size: 12px; }
.notice span { color: var(--muted); }
.notice strong { color: var(--ink); }
table { width: 100%; border-collapse: collapse; overflow: hidden; font-size: 12px; }
th { color: var(--muted); font-size: 10px; text-transform: uppercase; letter-spacing: 0.08em; text-align: left; padding: 11px 10px; background: rgba(255,255,255,0.025); border-bottom: 1px solid var(--line); }
td { padding: 13px 10px; border-bottom: 1px solid rgba(108, 140, 158, 0.1); color: #cbdbe2; }
tbody tr { cursor: pointer; }
tbody tr:hover td { background: rgba(32, 230, 208, 0.045); }
.path { color: var(--muted-2); font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 11px; overflow-wrap: anywhere; }
.audit-row { display: grid; grid-template-columns: 70px 1fr auto; align-items: center; gap: 12px; padding: 12px 13px; margin-bottom: 10px; border-radius: 14px; background: rgba(7, 18, 25, 0.7); border: 1px solid var(--line); }
.audit-code { color: var(--cyan); font-weight: 950; font-size: 12px; }
.audit-name { font-size: 12px; color: #cfdee5; }
.audit-wide { align-items: stretch; }
.audit-kpi-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(180px, 1fr));
  gap: 14px;
  margin-bottom: 14px;
}
.audit-compact-panel { min-height: 0; min-width: 0; }
.audit-compact-panel .panel-body { padding: 14px 16px; }
.audit-run-selector { margin-bottom: 14px; }
.audit-selector-body { display: grid; grid-template-columns: auto auto auto minmax(180px, 1fr) minmax(0, 0.8fr); gap: 10px; align-items: center; min-width: 0; }
.audit-run-select {
  width: 100%;
  min-width: 0;
  min-height: 36px;
  border-radius: 10px;
  border: 1px solid rgba(108, 140, 158, 0.26);
  background: rgba(7, 18, 25, 0.88);
  color: var(--ink);
  padding: 0 12px;
  font-size: 12px;
  font-weight: 850;
}
.audit-selector-body .path {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.audit-lazy-note {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
  margin: 0 16px 14px;
  padding: 9px 11px;
  border-radius: 12px;
  border: 1px solid rgba(76, 201, 240, 0.2);
  background: rgba(76, 201, 240, 0.06);
  color: var(--muted);
  font-size: 11px;
  font-weight: 850;
}
.audit-lazy-note span {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.small-action {
  min-height: 34px;
  border-radius: 10px;
  border: 1px solid rgba(32, 230, 208, 0.22);
  background: rgba(32, 230, 208, 0.09);
  color: var(--ink);
  padding: 0 12px;
  font-size: 12px;
  font-weight: 900;
  cursor: pointer;
}
.small-action.primary { background: linear-gradient(135deg, rgba(32,230,208,.9), rgba(76,201,240,.75)); color: #05141a; }
.small-action:disabled { opacity: 0.42; cursor: not-allowed; }
.tag-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
.audit-line-summary-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(280px, 1fr));
  gap: 14px;
  align-items: start;
  margin-bottom: 14px;
}
.audit-line-card { min-width: 0; }
.audit-line-card .panel-header { min-height: 48px; min-width: 0; }
.audit-line-metrics {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
}
.audit-metric-box {
  min-width: 0;
  padding: 12px;
  border-radius: 12px;
  background: rgba(7, 18, 25, 0.74);
  border: 1px solid var(--line);
}
.audit-metric-box .plan-num { font-size: 24px; }
.audit-metric-box .plan-meta { min-width: 0; }
.audit-metric-box .plan-meta b { overflow-wrap: anywhere; text-align: right; }
.audit-step-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 10px;
  padding: 10px 11px;
  border-radius: 12px;
  background: rgba(7, 18, 25, 0.7);
  border: 1px solid var(--line);
}
.audit-step-row span:first-child { display: grid; gap: 3px; min-width: 0; }
.audit-step-row small { color: var(--muted-2); font-size: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; min-width: 0; overflow-wrap: anywhere; word-break: break-word; }
.audit-scope-note {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
  padding: 10px 12px;
  border-radius: 12px;
  border: 1px solid rgba(244, 200, 95, 0.22);
  background: rgba(244, 200, 95, 0.07);
  color: #ffe4a0;
  font-size: 11px;
  font-weight: 850;
}
.audit-health-grid { grid-template-columns: repeat(3, minmax(190px, 1fr)); }
.audit-run-list { display: grid; gap: 10px; max-height: 360px; overflow: auto; }
.audit-run-card { width: 100%; min-width: 0; display: grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr) auto; gap: 8px 12px; align-items: center; text-align: left; padding: 12px 14px; border-radius: 14px; border: 1px solid var(--line); background: rgba(7,18,25,.72); color: var(--ink); }
.audit-run-card.active { border-color: rgba(32,230,208,.36); background: rgba(32,230,208,.1); box-shadow: inset 3px 0 0 var(--cyan); }
.audit-run-card span, .audit-run-card small { color: var(--muted); font-size: 11px; }
.audit-run-card strong,
.audit-run-card span,
.audit-run-card small {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.audit-mini-steps { display: grid; gap: 8px; margin-top: 14px; }
.compact-tabs { gap: 6px; }
.compact-tabs button { height: 30px; padding: 0 10px; font-size: 11px; }
.audit-symbol-table { max-height: 520px; overflow: auto; }
.candidate-governance-table table { min-width: 1180px; }
.candidate-governance-table th:first-child,
.candidate-governance-table td:first-child {
  position: sticky;
  left: 0;
  z-index: 2;
  background: #071923;
}
.candidate-governance-table th:first-child { z-index: 3; }
.audit-log-box { max-height: 280px; overflow: auto; }
.heatmap { display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; }
.heat-cell { min-height: 58px; padding: 10px; border-radius: 14px; border: 1px solid rgba(255,255,255,0.06); background: rgba(255,255,255,0.035); }
.heat-cell.good { background: rgba(33,212,136,0.14); border-color: rgba(33,212,136,0.18); }
.heat-cell.bad { background: rgba(255,95,115,0.14); border-color: rgba(255,95,115,0.18); }
.heat-cell.hot { background: rgba(255,159,67,0.14); border-color: rgba(255,159,67,0.18); }
.heat-symbol { color: var(--muted); font-size: 10px; font-weight: 900; }
.heat-score { margin-top: 7px; font-size: 16px; font-weight: 950; }
.meter-ring { width: 150px; height: 150px; margin: 2px auto 14px; border-radius: 50%; display: grid; place-items: center; background: radial-gradient(circle at center, var(--surface-2) 0 53%, transparent 54%), conic-gradient(var(--green) 0 41%, var(--amber) 41% 64%, rgba(126,148,163,0.14) 64% 100%); box-shadow: inset 0 0 28px rgba(0,0,0,0.4), 0 0 28px rgba(33,212,136,0.11); }
.meter-ring strong { font-size: 30px; letter-spacing: -0.05em; }
.meter-ring span { display: block; margin-top: 3px; color: var(--muted); font-size: 10px; text-align: center; text-transform: uppercase; letter-spacing: 0.08em; }
.risk-row { display: grid; grid-template-columns: 1fr auto; gap: 12px; padding: 10px 0; border-bottom: 1px solid rgba(108,140,158,0.1); color: var(--muted); font-size: 12px; }
.risk-row strong { color: var(--ink); }
.log-list { display: grid; gap: 9px; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 11px; color: #a9bdc8; }
.log-line { padding: 10px 11px; border-radius: 12px; background: rgba(7, 18, 25, 0.75); border: 1px solid var(--line); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.alert { margin-bottom: 14px; padding: 14px; border-radius: 14px; border: 1px solid var(--line); }
.alert.bad { color: #ffc1c9; background: rgba(255, 95, 115, 0.1); border-color: rgba(255, 95, 115, 0.3); }
.alert.info { color: var(--cyan); background: rgba(45, 222, 255, 0.08); border-color: rgba(45, 222, 255, 0.26); }
.action-row { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; }
.pipeline-page { display: grid; gap: 14px; }
.pipeline-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 2px; }
.pipeline-header h2 { margin: 0; font-size: 24px; line-height: 1.1; letter-spacing: -0.04em; font-weight: 950; }
.pipeline-header p { margin: 8px 0 0; color: var(--muted); font-size: 13px; max-width: 860px; }
.pipeline-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
.pipeline-layout { display: grid; grid-template-columns: minmax(0, 1.5fr) minmax(320px, 0.9fr); gap: 14px; margin-bottom: 14px; }
.strategy-stack { display: grid; gap: 12px; }
.strategy-card {
  background: linear-gradient(180deg, rgba(7,25,35,.96), rgba(4,18,27,.98));
  border: 1px solid rgba(65,120,145,.18);
  border-radius: 18px;
  padding: 16px;
  box-shadow: inset 0 1px 0 rgba(255,255,255,.02);
}
.strategy-top { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 12px; }
.strategy-name { font-size: 15px; font-weight: 900; margin-bottom: 4px; }
.strategy-desc { color: var(--muted); font-size: 12px; line-height: 1.5; }
.strategy-num { text-align: right; }
.strategy-percent { font-size: 24px; font-weight: 950; letter-spacing: -0.05em; }
.strategy-stage { color: var(--muted); font-size: 11px; margin-top: 4px; }
.strategy-meta-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  margin-top: 10px;
  color: var(--muted-2);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 10px;
}
.strategy-meta-row span { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.progress-track {
  height: 12px;
  border-radius: 999px;
  background: rgba(40,71,88,.34);
  border: 1px solid rgba(72,111,132,.18);
  overflow: hidden;
  position: relative;
  margin-bottom: 14px;
}
.progress-fill {
  height: 100%;
  width: 0%;
  border-radius: inherit;
  position: relative;
  transition: width .22s linear;
  background: linear-gradient(90deg, #30d9ff 0%, #1caee0 100%);
  box-shadow: 0 0 18px rgba(48,217,255,.18);
}
.progress-fill.fast { background: linear-gradient(90deg, #39f09d 0%, #23c37d 100%); box-shadow: 0 0 18px rgba(41,227,138,.18); }
.progress-fill.full { background: linear-gradient(90deg, #f8b24a 0%, #f0902b 100%); box-shadow: 0 0 18px rgba(246,163,61,.18); }
.progress-fill::after {
  content: "";
  position: absolute;
  inset: 0;
  background-image: linear-gradient(45deg, rgba(255,255,255,.22) 25%, transparent 25%, transparent 50%, rgba(255,255,255,.22) 50%, rgba(255,255,255,.22) 75%, transparent 75%, transparent);
  background-size: 18px 18px;
  animation: stripe 1s linear infinite;
  opacity: 0;
}
.running .progress-fill::after { opacity: 1; }
@keyframes stripe { from { background-position: 0 0; } to { background-position: 18px 0; } }
.stage-row { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 8px; }
.stage {
  min-height: 52px;
  padding: 8px 8px 9px;
  border-radius: 12px;
  border: 1px solid rgba(65,120,145,.16);
  background: linear-gradient(180deg, rgba(8,28,39,.86), rgba(6,21,30,.96));
  color: var(--muted-2);
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  transition: .18s ease;
}
.stage b { font-size: 11px; color: #b7d2dd; margin-bottom: 3px; }
.stage span { font-size: 10px; }
.stage small {
  min-height: 12px;
  color: rgba(164,190,204,.78);
  font-size: 9px;
  line-height: 1.15;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.stage.done {
  background: linear-gradient(180deg, rgba(13,61,50,.78), rgba(8,40,33,.92));
  border-color: rgba(41,227,138,.2);
  color: #87dcb2;
}
.stage.done b { color: #d8fff0; }
.stage.blocked {
  background: linear-gradient(180deg, rgba(74,49,12,.84), rgba(42,28,10,.94));
  border-color: rgba(244,200,95,.32);
  color: #ffd978;
  box-shadow: 0 0 18px rgba(244,200,95,.08);
}
.stage.blocked b { color: #fff1bd; }
.stage.failed {
  background: linear-gradient(180deg, rgba(76,25,34,.84), rgba(40,12,18,.94));
  border-color: rgba(255,95,115,.32);
  color: #ffc5ce;
}
.stage.failed b { color: #ffe2e7; }
.stage.skipped {
  opacity: 0.48;
  border-color: rgba(108,140,158,.12);
  background: rgba(8,24,34,.6);
}
.stage.active {
  background: linear-gradient(180deg, rgba(10,58,72,.82), rgba(8,36,48,.94));
  border-color: rgba(43,216,243,.24);
  color: #92eaf9;
  box-shadow: 0 0 18px rgba(43,216,243,.08);
  transform: translateY(-1px);
}
.stage.active b { color: #e7fcff; }
.stage.funnel-empty {
  border-color: rgba(244,200,95,.28);
  background: linear-gradient(180deg, rgba(53,42,14,.7), rgba(23,24,16,.9));
}
.stage.funnel-blocked,
.stage.funnel-breakpoint {
  border-color: rgba(255,95,115,.42);
  background: linear-gradient(180deg, rgba(70,24,33,.78), rgba(34,12,18,.94));
  box-shadow: 0 0 0 1px rgba(255,95,115,.08), 0 0 16px rgba(255,95,115,.06);
}
.stage.funnel-stale {
  border-color: rgba(244,200,95,.32);
}
.stage.funnel-na {
  opacity: .42;
  background: rgba(8,24,34,.48);
}
.funnel-breakpoint-row {
  color: #ffd978;
}
.strategy-card.line-state-blocked {
  border-color: rgba(244,200,95,.22);
}
.strategy-card.line-state-failed {
  border-color: rgba(255,95,115,.22);
}
.strategy-card.line-state-completed {
  border-color: rgba(33,212,136,.18);
}
.strategy-card.line-state-skipped {
  opacity: 0.62;
  border-color: rgba(108,140,158,.14);
}
.side-stack { display: grid; gap: 14px; align-content: start; }
.mode-list { display: grid; gap: 10px; }
.mode-item {
  border: 1px solid rgba(65,120,145,.16);
  border-radius: 14px;
  background: linear-gradient(180deg, rgba(8,28,39,.86), rgba(6,21,30,.96));
  padding: 12px 13px;
}
.mode-item strong { display: flex; align-items: center; justify-content: space-between; font-size: 13px; margin-bottom: 5px; }
.mode-item p { margin: 0; color: var(--muted); font-size: 12px; line-height: 1.5; }
.stats-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.stat {
  border: 1px solid rgba(65,120,145,.16);
  border-radius: 14px;
  background: linear-gradient(180deg, rgba(8,28,39,.86), rgba(6,21,30,.96));
  padding: 12px 10px;
}
.stat span { display: block; color: var(--muted); font-size: 10px; text-transform: uppercase; letter-spacing: .08em; font-weight: 900; margin-bottom: 7px; }
.stat strong { font-size: 20px; font-weight: 950; letter-spacing: -0.04em; }
.log-box {
  height: 286px;
  overflow: auto;
  border-radius: 14px;
  border: 1px solid rgba(65,120,145,.16);
  background: linear-gradient(180deg, rgba(6,22,31,.95), rgba(5,18,26,.98));
  padding: 10px 12px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 11px;
}
.log-box .log-line { padding: 7px 0; border-bottom: 1px solid rgba(255,255,255,.05); color: #b9d8e4; white-space: nowrap; }
.log-ok { color: #72ebb0; }
.log-warn { color: #ffd978; }
.footer-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
.btn.stop {
  background: linear-gradient(180deg, rgba(64,28,34,.98), rgba(40,14,19,.98));
  border-color: rgba(255,107,120,.22);
  color: #ffd9de;
}
.paper-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin-bottom: 14px; }
.tabs { display: inline-flex; gap: 6px; padding: 6px; margin-bottom: 14px; border-radius: 16px; border: 1px solid var(--line); background: rgba(7, 18, 25, 0.68); }
.paper-toolbar .tabs { margin-bottom: 0; }
.tabs.slim button { min-height: 28px; font-size: 11px; }
.tabs button { border: 0; background: transparent; color: var(--muted); border-radius: 11px; min-height: 32px; padding: 0 12px; font-weight: 850; }
.tabs button.active { color: #061115; background: linear-gradient(135deg, var(--cyan), var(--blue)); }
.paper-grid { display: grid; grid-template-columns: 1.35fr 1fr; gap: 14px; margin-bottom: 14px; }
.paper-grid > .panel:nth-child(-n + 2) .panel-body {
  max-height: 900px;
  overflow-y: auto;
  overflow-x: auto;
  scrollbar-color: rgba(32, 230, 208, 0.45) rgba(7, 18, 25, 0.72);
  scrollbar-width: thin;
}
.paper-grid > .panel:nth-child(-n + 2) .panel-body::-webkit-scrollbar {
  width: 10px;
  height: 10px;
}
.paper-grid > .panel:nth-child(-n + 2) .panel-body::-webkit-scrollbar-track {
  background: rgba(7, 18, 25, 0.72);
  border-radius: 999px;
}
.paper-grid > .panel:nth-child(-n + 2) .panel-body::-webkit-scrollbar-thumb {
  background: linear-gradient(180deg, rgba(32, 230, 208, 0.58), rgba(46, 168, 255, 0.38));
  border-radius: 999px;
  border: 2px solid rgba(7, 18, 25, 0.72);
}
.paper-grid > .panel:nth-child(-n + 2) th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: #132832;
}
.paper-grid > .panel:nth-child(-n + 2) tbody tr {
  height: 40px;
}
.config-profile-panel { margin-bottom: 14px; }
.config-governance-panel { margin-bottom: 14px; }
.config-governance-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}
.config-tab-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}
.segment-btn {
  min-height: 34px;
  border: 1px solid rgba(108,140,158,0.18);
  border-radius: 999px;
  color: var(--muted);
  background: rgba(7, 18, 25, 0.64);
  padding: 0 13px;
  font-size: 12px;
  font-weight: 850;
}
.segment-btn.active {
  color: #061218;
  border-color: transparent;
  background: linear-gradient(135deg, var(--cyan), #2ea8ff);
}
.config-tab-hint {
  margin-top: 10px;
  color: var(--muted);
  font-size: 12px;
}
.config-impact-mini {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-left: auto;
}
.config-warning {
  margin-bottom: 12px;
}
.config-effective-card,
.config-impact-list {
  display: grid;
  gap: 10px;
  margin-bottom: 12px;
  padding: 12px;
  border: 1px solid rgba(32, 230, 208, 0.16);
  border-radius: 14px;
  background: rgba(7, 28, 32, 0.44);
}
.config-preview-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}
.config-impact-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  align-items: center;
  gap: 8px;
  min-height: 34px;
  padding: 7px 9px;
  border: 1px solid rgba(108,140,158,0.14);
  border-radius: 10px;
  background: rgba(7, 18, 25, 0.58);
  color: var(--muted);
  font-size: 11px;
}
.config-impact-row span:first-child {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.legacy-shield {
  opacity: 0.82;
}
.profile-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}
.profile-card {
  min-height: 86px;
  border: 1px solid var(--line);
  border-radius: 16px;
  padding: 14px;
  color: var(--ink);
  background: linear-gradient(180deg, rgba(7, 18, 25, 0.78), rgba(8, 24, 34, 0.94));
  text-align: left;
}
.profile-card.active {
  border-color: rgba(32, 230, 208, 0.38);
  box-shadow: inset 3px 0 0 var(--cyan), 0 0 24px rgba(32, 230, 208, 0.14);
}
.profile-card strong { display: block; margin-bottom: 8px; font-size: 14px; font-weight: 950; }
.profile-card span { color: var(--muted); font-size: 11px; line-height: 1.5; }
.config-message-row {
  margin-top: 14px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.config-line-grid { margin-bottom: 14px; }
.config-section-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
}
.config-editor-card .panel-body { padding: 12px; }
.micro-policy-controls {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
  padding: 12px;
  border: 1px solid rgba(32, 230, 208, 0.16);
  border-radius: 14px;
  background: rgba(7, 28, 32, 0.58);
}
.micro-policy-controls label {
  min-width: 0;
}
.micro-policy-controls select {
  min-height: 38px;
  border-radius: 12px;
  border: 1px solid var(--line);
  color: var(--ink);
  background: rgba(7, 18, 25, 0.84);
  padding: 0 12px;
}
.micro-policy-controls .toggle-row {
  display: flex;
  align-items: center;
  gap: 9px;
  min-height: 38px;
  padding: 0 10px;
  border-radius: 12px;
  border: 1px solid rgba(108,140,158,0.14);
  background: rgba(7, 18, 25, 0.58);
}
.micro-policy-controls .toggle-row input {
  min-height: 0;
  width: 16px;
  height: 16px;
  padding: 0;
}
.market-now-controls {
  display: grid;
  gap: 10px;
  margin-bottom: 12px;
  padding: 12px;
  border: 1px solid rgba(245, 196, 89, 0.18);
  border-radius: 14px;
  background: rgba(29, 24, 10, 0.35);
}
.market-now-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}
.market-now-header strong,
.market-now-side-title {
  font-size: 12px;
  font-weight: 950;
  color: var(--ink);
}
.market-now-side-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
}
.market-now-side {
  display: grid;
  gap: 8px;
  min-width: 0;
  padding: 10px;
  border-radius: 12px;
  border: 1px solid rgba(108,140,158,0.14);
  background: rgba(7, 18, 25, 0.58);
}
.market-now-side label {
  display: grid;
  grid-template-columns: minmax(90px, 1fr) minmax(72px, 96px);
  align-items: center;
  gap: 8px;
  min-width: 0;
  font-size: 11px;
  color: var(--muted);
}
.market-now-side input[type="number"] {
  min-height: 32px;
  border-radius: 10px;
  border: 1px solid var(--line);
  color: var(--ink);
  background: rgba(7, 18, 25, 0.84);
  padding: 0 8px;
}
.market-now-controls .toggle-row {
  display: flex;
  align-items: center;
  gap: 8px;
  min-height: 32px;
}
.market-now-controls .toggle-row input {
  min-height: 0;
  width: 16px;
  height: 16px;
  padding: 0;
}
.config-textarea {
  width: 100%;
  min-height: 420px;
  resize: vertical;
  border: 1px solid rgba(108,140,158,0.18);
  border-radius: 14px;
  padding: 12px;
  color: #d7e7ee;
  background: rgba(7, 18, 25, 0.72);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 11px;
  line-height: 1.5;
  outline: none;
}
.config-textarea.compact { min-height: 300px; }
.config-textarea:focus {
  border-color: rgba(32, 230, 208, 0.38);
  box-shadow: 0 0 0 3px rgba(32, 230, 208, 0.08);
}
.stat-cards { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
.stat small { display: block; margin-top: 8px; color: var(--muted); font-size: 11px; line-height: 1.5; }
.empty-state { color: var(--muted); text-align: center; padding: 22px 10px; }
.trade-detail-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; margin-bottom: 14px; }
.trade-detail-grid > div { padding: 12px; border-radius: 14px; border: 1px solid rgba(65,120,145,.16); background: rgba(7, 18, 25, 0.7); }
.trade-detail-grid span { display: block; color: var(--muted); font-size: 10px; font-weight: 900; letter-spacing: .08em; text-transform: uppercase; margin-bottom: 7px; }
.trade-detail-grid strong { font-size: 14px; }
.level-list { display: grid; gap: 10px; align-content: start; }
.form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.package-meta-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;
  border-top: 1px solid rgba(108, 140, 158, 0.12);
}
.package-meta-grid .stat { min-height: 96px; }
label { display: grid; gap: 8px; color: var(--muted); font-size: 11px; font-weight: 850; letter-spacing: 0.06em; text-transform: uppercase; }
input,
select {
  min-height: 38px;
  width: 100%;
  min-width: 0;
  border-radius: 12px;
  border: 1px solid rgba(65, 120, 145, 0.26);
  color: var(--ink);
  background:
    linear-gradient(180deg, rgba(9, 28, 39, 0.92), rgba(6, 18, 26, 0.96));
  padding: 0 12px;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03), 0 10px 24px rgba(0, 0, 0, 0.12);
  outline: none;
}
select {
  appearance: none;
  -webkit-appearance: none;
  padding-right: 36px;
  cursor: pointer;
  background-image:
    linear-gradient(45deg, transparent 50%, rgba(32, 230, 208, 0.9) 50%),
    linear-gradient(135deg, rgba(32, 230, 208, 0.9) 50%, transparent 50%),
    linear-gradient(180deg, rgba(9, 28, 39, 0.92), rgba(6, 18, 26, 0.96));
  background-position:
    calc(100% - 18px) 16px,
    calc(100% - 12px) 16px,
    0 0;
  background-size:
    6px 6px,
    6px 6px,
    100% 100%;
  background-repeat: no-repeat;
}
select option {
  color: var(--ink);
  background: #07131c;
}
input::placeholder { color: rgba(139, 166, 180, 0.62); }
input:focus,
select:focus {
  border-color: rgba(32, 230, 208, 0.72);
  box-shadow: 0 0 0 3px rgba(32, 230, 208, 0.09), inset 0 1px 0 rgba(255, 255, 255, 0.04);
}
pre { margin: 0; max-height: 460px; overflow: auto; padding: 12px; border-radius: 14px; border: 1px solid rgba(108,140,158,0.12); background: rgba(7, 18, 25, 0.66); color: #a9bdc8; font-size: 11px; line-height: 1.5; }
.modal-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,0.62); display: grid; place-items: center; z-index: 50; padding: 24px; }
.modal { width: min(1120px, 96vw); max-height: 90vh; overflow: auto; }
.chart-box { min-height: 260px; display: flex; align-items: center; gap: 4px; padding: 20px; border-radius: 16px; border: 1px solid var(--line); background: rgba(4, 12, 18, 0.66); margin-bottom: 14px; }
.candle { width: 8px; min-height: 8px; border-radius: 999px; align-self: center; }
.up-candle { background: var(--green); box-shadow: 0 0 10px rgba(33,212,136,0.35); }
.down-candle { background: var(--red); box-shadow: 0 0 10px rgba(255,95,115,0.35); }
@media (max-width: 1280px) {
  .grid-kpi { grid-template-columns: repeat(2, 1fr); }
  .dashboard-grid, .wide-grid, .bottom-grid, .line-grid, .pipeline-layout, .footer-grid, .paper-grid, .stat-cards, .trade-detail-grid, .config-section-grid { grid-template-columns: 1fr; }
  .package-meta-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .audit-kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .audit-line-summary-grid { grid-template-columns: repeat(3, minmax(230px, 1fr)); }
  .audit-selector-body { grid-template-columns: auto auto auto minmax(220px, 1fr); }
  .audit-selector-body .path { grid-column: 1 / -1; }
  .hero { grid-template-columns: 1fr; }
  .market-strip { overflow-x: auto; }
}
@media (max-width: 860px) {
  :root { --sidebar: 0px; }
  .app { grid-template-columns: 1fr; }
  .sidebar { display: none; }
  .topbar { grid-column: 1; padding: 0 16px; }
  .content { grid-column: 1; padding: 18px 16px 28px; }
  .grid-kpi, .plan-grid, .form-grid, .profile-grid { grid-template-columns: 1fr; }
  .package-meta-grid { grid-template-columns: 1fr; }
  .audit-kpi-grid, .audit-line-summary-grid, .audit-line-metrics, .audit-selector-body { grid-template-columns: 1fr; }
  .audit-selector-body .path { grid-column: auto; }
  .pipeline-header { flex-direction: column; }
  .pipeline-actions { justify-content: flex-start; }
  .stage-row, .stats-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .identity, .topbar .chip, .topbar .btn:not(.primary) { display: none; }
}
</style>
