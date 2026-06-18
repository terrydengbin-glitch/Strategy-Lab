# Strategy Lab

本仓库实现 **Strategy Lab**：可被 Orchestrator 同步调用的策略研究、回测、paper 模拟与沙盒实验组件。

Python 包目录名：`laoma_signal_engine`（与技术栈文档一致）。

## 本地环境（Windows PowerShell 示例）

```powershell
cd e:\collector\traders\Strategy-Lab
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -e .
python -m laoma_signal_engine --help
```

环境变量建议：`PYTHONUTF8=1`（见项目规则）。

## 目录约定

- 源码：`laoma_signal_engine/`
- 运行期数据：`DATA/`（不提交本地交易数据、SQLite、沙盒产物、日志或审计包）
- 前端控制台：`web/`

## 当前 CLI 形态

```text
python -m laoma_signal_engine run --mode once --stdout-json
python -m laoma_signal_engine run-pipeline --stdout-json
python -m laoma_signal_engine run-pipeline-with-micro --micro-wait-until-ready --stdout-json
python -m laoma_signal_engine build-universe --force
python -m laoma_signal_engine fetch-futures-light-snapshot
python -m laoma_signal_engine scan --stdout-json
python -m laoma_signal_engine route-micro-targets --stdout-json
python -m laoma_signal_engine assemble-factor-snapshot --stdout-json
python -m laoma_signal_engine assemble-factor-snapshot-without-ofi-cvd --stdout-json
python -m laoma_signal_engine apply-direction-gate --stdout-json
python -m laoma_signal_engine apply-final-decisions --stdout-json
python -m laoma_signal_engine run-llm-factor-assist --stdout-json --max-factor-items 2
```

**Step 1（`build-universe`）已实现**：从 Binance 拉 Spot/Futures `exchangeInfo` 与 Futures `ticker/24hr`，生成 `DATA/universe/CANDIDATE_UNIVERSE.json`。输出含顶层 `counts`（区分 595 全量 vs `futures_count` 永续筛选池），每条 pair 含 `display_base_asset` / `cashtag` / `spot_cashtag_symbol` / `symbol_safe_id` / `eligible_for_signal_engine`；`eligible_for_post` 仅在「现货 + 永续」同时为真时为真，**spot-only 恒为 false**（无永续腿不参与 futures-first 发帖模板）。另有 `neither_spot_nor_futures`（手工占位、无两腿时计入；全量 Binance 并集常为 0）。

**Step 1.5（`fetch-futures-light-snapshot`）已实现**：读取 Universe，对 eligible 永续拉 1m/15m/1h K 线与全市场 `ticker/24hr`，写 `DATA/market/futures_light_snapshot.json`（顶层含 `universe_count` / `eligible_futures_count` / `success_count` / `failed_count` / `skipped_count`）。请求参数见 `laoma_signal_engine/config/light_snapshot_fetch.yaml`。调试示例：

```text
python -m laoma_signal_engine fetch-futures-light-snapshot --limit 50
python -m laoma_signal_engine fetch-futures-light-snapshot --symbols BTCUSDT,ETHUSDT
python -m laoma_signal_engine fetch-futures-light-snapshot --max-concurrency 8
python -m laoma_signal_engine fetch-futures-light-snapshot --fetch-mode legacy
python -m laoma_signal_engine fetch-futures-light-snapshot --dry-run-plan --limit 529
```

**Step 1.51（asyncio + 限流）**：单进程 `asyncio` + 共享 `httpx.AsyncClient` + IP 权重响应头反压；**主 JSON schema 与 Step 1.5 相同**；性能一行写入 `DATA/logs/light_snapshot_perf.jsonl`。**CLI 与 `run_fetch_light_snapshot` 默认 `--fetch-mode async`**（与 `pipeline.LIGHT_SNAPSHOT_FETCH_MODE_DEFAULT` 一致）；需线程池旧路径时使用 **`--fetch-mode legacy`**。

符号列表仍可用 `futures_symbols_for_light_snapshot(doc)` / `futures_symbols_for_step_1_5(doc)`；**不要用 Universe 顶层 `count` 当扫描条数**。

**结论（Step 1 -> Step 1.5）**：Step 1 的基础币表已经合格；Step 1.5 只要严格读取 `has_um_futures=true` 的 529 个（与当次 `counts.futures_count` 一致，会随 Binance 上币/下架变化），就可以继续做 15m 轻量扫描。代码侧过滤条件为 `has_um_futures and eligible_for_trade_analysis`；在当前 Step 1 构建逻辑里二者与永续行一一对应，已与本地 `CANDIDATE_UNIVERSE.json` 交叉校验（`futures_count`、仅永续行计数、Step 1.5 符号列表长度三者相同）。

**Step 2（`scan`）已实现**：读取 `DATA/market/futures_light_snapshot.json`，输出 `DATA/raw_signals/latest_raw_candidates.json`、`latest_watch_signals.json`、`latest_strong_candidates.json`；Step 2.1 新鲜度门禁已接入，开发态可用 `--allow-stale-input`。

**Step 2.5（`route-micro-targets`）已实现**：读取 Step 2 三档候选，输出 `DATA/micro/micro_targets.json`；默认不把 raw 全量送入 micro，且 stale 输入会被挡住，避免过期候选触发订阅。

**Step 3 / Micro Collector 已实现为内部编排与 daemon 包**：`laoma_signal_engine/micro/daemon/`、`micro/ws/`、`micro/wait_until_ready/` 已落地；顶层 `micro-collector` 子命令仍保留为 stub，生产/联调入口目前使用 `run-pipeline-with-micro` 或 `scripts/run_micro_until_ready.py`。

**Step 3B / Step 3B.1 已实现**：`assemble-factor-snapshot` 组装含 micro 的 `DATA/factors/latest_factor_snapshot.json`；`assemble-factor-snapshot-without-ofi-cvd` 组装无 OFI/CVD 的 `DATA/factors/latest_factor_snapshot_withoutoficvd.json`。默认接 STEP4.1 的 OI / Funding / Basis 真数；`--skip-market-context` 可保留占位。

**Step 4 / STEP4.1 已实现**：`apply-direction-gate` 输出 `DATA/decisions/latest_direction_decisions.json`；默认要求 context guards ready 才允许 NOW，`--disable-context-guards-for-now` 仅供 smoke。

**Step 5.0 已实现**：`apply-final-decisions` 读取 Direction + Factor + Light Snapshot，输出 Orchestrator 消费的 `DATA/decisions/latest_decisions.json`；包含 `risk_plan`、`decisions[]`、`rejected[]`，并接入 SL/TP Planner 与 RiskGate。

**Step 6.0 LLM 辅助已实现**：`run-llm-factor-assist` 读取两份 factor snapshot 与 `DATA/llm/prompts/*.txt`，调用 DeepSeek 后写 `DATA/llm/out/llm_out_*.json`。需要 `DEEPSEEK_API_KEY`；默认测试使用 mock，不发真网。

**manual_watchlist.json**：可复制 `DATA/universe/manual_watchlist.example.json` 为 `manual_watchlist.json`。文件若损坏或 JSON 无效，程序会 **记 WARN 并忽略该文件**（不中断构建）。

当前占位/未完成入口：`decide`、顶层 `micro-collector` 子命令、`run --mode loop`。`run --mode once` 已映射到 `run-pipeline`。

当前默认测试基线：`python -m pytest` 为无真网测试；最近本地验证结果为 `272 passed, 1 deselected`。
