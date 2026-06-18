"""Paper-equivalent execution adapter for historical backtests.

This module is intentionally a thin orchestration layer around the P14 paper
engine.  Backtest runners can feed historical trade-plan documents and candles
through the same adapter/gate/order/fill contract used by paper without writing
to the live paper ledger.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from laoma_signal_engine.backtest.execution_contract import paper_equivalent_metadata
from laoma_signal_engine.candidate_ledger import sync_candidate_ledger_from_paper_sqlite
from laoma_signal_engine.paper.engine import PaperEngine
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.training_snapshot_sync import sync_paper_sqlite_source
from laoma_signal_engine.trade_quality.auto_completion import complete_paper_trade_quality


EXECUTION_CONTRACT = "paper_equivalent"
EXECUTION_CONTRACT_VERSION = "step7.145.v1"
PAPER_ADAPTER_VERSION = "paper.adapter.v1"
PAPER_GATE_VERSION = "paper_v5_trade_gate_v1"


class HistoricalCandleProvider:
    """Deterministic 1m candle provider with an explicit time cursor."""

    historical_time_mode = True

    def __init__(self, candles_by_symbol: Mapping[str, Iterable[Candle | Mapping[str, Any]]]) -> None:
        self._candles_by_symbol: dict[str, list[Candle]] = {
            str(symbol).upper(): sorted((_as_candle(str(symbol), row) for row in rows), key=lambda item: item.open_time_ms)
            for symbol, rows in candles_by_symbol.items()
        }
        self._timeline = sorted({row.open_time_ms for rows in self._candles_by_symbol.values() for row in rows})
        self._current_open_time_ms: int | None = None

    @property
    def timeline(self) -> list[int]:
        return list(self._timeline)

    @property
    def current_open_time_ms(self) -> int | None:
        return self._current_open_time_ms

    def advance_to(self, open_time_ms: int) -> None:
        self._current_open_time_ms = int(open_time_ms)

    def get_1m(self, symbol: str, *, limit: int = 5) -> list[Candle]:
        rows = self._candles_by_symbol.get(str(symbol).upper(), [])
        if self._current_open_time_ms is None:
            return rows[-int(limit) :]
        eligible = [row for row in rows if row.open_time_ms <= self._current_open_time_ms]
        return eligible[-int(limit) :]


class PaperEquivalentBacktestSession:
    """Stateful paper-equivalent runner for chronological historical replay."""

    def __init__(
        self,
        project_root: Path,
        *,
        run_id: str,
        candles_by_symbol: Mapping[str, Iterable[Candle | Mapping[str, Any]]],
        config: PaperConfig | None = None,
    ) -> None:
        self.project_root = Path(project_root)
        self.run_id = str(run_id)
        self.provider = HistoricalCandleProvider(candles_by_symbol)
        self.config = config or default_paper_equivalent_config(run_id=self.run_id)
        self.engine = PaperEngine(self.project_root, config=self.config, candle_provider=self.provider)
        self.engine.initialize()
        self._timeline_index = 0
        self.consume_results: list[dict[str, Any]] = []
        self.tick_results: list[dict[str, Any]] = []

    def advance_until(self, open_time_ms: int) -> None:
        target = int(open_time_ms)
        timeline = self.provider.timeline
        while self._timeline_index < len(timeline) and timeline[self._timeline_index] <= target:
            self._process_tick(timeline[self._timeline_index])
            self._timeline_index += 1

    def consume_trade_plan(self, docs: dict[str, dict[str, Any]], *, at_ms: int | None = None) -> dict[str, Any]:
        if at_ms is not None:
            self.advance_until(int(at_ms))
        result = self.engine.consume_trade_plans(docs)
        self.consume_results.append(result)
        return result

    def finish(self) -> dict[str, Any]:
        timeline = self.provider.timeline
        while self._timeline_index < len(timeline):
            self._process_tick(timeline[self._timeline_index])
            self._timeline_index += 1
        summary = self.engine.store.write_summary()
        metadata = paper_equivalent_metadata(
            execution_contract_version=EXECUTION_CONTRACT_VERSION,
            paper_adapter_version=PAPER_ADAPTER_VERSION,
            paper_gate_version=PAPER_GATE_VERSION,
            paper_fill_model=self.config.fill_model_mode,
        )
        result = {
            "schema_version": "step7.145.paper_equivalent_session.v1",
            **metadata,
            "paper_equivalent_run_id": self.run_id,
            "db_path": str(self.engine.store.db_path),
            "summary_path": str(self.engine.store.summary_path),
            "consume_results": self.consume_results,
            "ticks": self.tick_results,
            "counts": _ledger_counts(self.engine),
            "summary": {
                "stats": summary.get("stats"),
                "summary": summary.get("summary"),
            },
        }
        result["trade_quality_completion"] = complete_paper_trade_quality(
            self.project_root,
            config=self.config,
            candle_provider=self.provider,
        )
        result["training_dataset"] = sync_paper_sqlite_source(
            self.project_root,
            source_db_path=self.engine.store.db_path,
            run_id=f"paper_equivalent_{self.run_id}",
            source_mode="paper_equivalent_backtest",
        )
        result["candidate_ledger"] = sync_candidate_ledger_from_paper_sqlite(
            self.project_root,
            source_db_path=self.engine.store.db_path,
            run_id=f"paper_equivalent_{self.run_id}",
            source_mode="baseline_backtest",
            docs=None,
            counterfactual_candles_by_symbol=None,
            execution_source="paper_equivalent_backtest",
        )
        return result

    def _process_tick(self, open_time_ms: int) -> None:
        self.provider.advance_to(open_time_ms)
        entries = self.engine.process_pending_entries()
        closes = self.engine.process_open_positions()
        self.tick_results.append(
            {
                "open_time_ms": open_time_ms,
                "entries": entries,
                "closes": closes,
            }
        )


def default_paper_equivalent_config(*, run_id: str, base: PaperConfig | None = None) -> PaperConfig:
    """Return an isolated PaperConfig for backtest execution."""

    cfg = base or PaperConfig()
    return replace(
        cfg,
        db_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_equivalent.db",
        summary_path=f"DATA/backtest/paper_equivalent/{run_id}/latest_paper_equivalent_state.json",
        max_trade_plan_age_sec=0,
        daemon_lock_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_daemon.lock",
        daemon_pid_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_daemon.pid",
        daemon_log_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_daemon.log",
        daemon_heartbeat_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_daemon_heartbeat.json",
        daemon_status_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_daemon_status.json",
        archive_dir=f"DATA/backtest/paper_equivalent/{run_id}/archives",
        archive_metadata_path=f"DATA/backtest/paper_equivalent/{run_id}/paper_experiments.json",
    )


def run_paper_equivalent_backtest(
    project_root: Path,
    *,
    docs: dict[str, dict[str, Any]],
    candles_by_symbol: Mapping[str, Iterable[Candle | Mapping[str, Any]]],
    run_id: str | None = None,
    config: PaperConfig | None = None,
    max_ticks: int | None = None,
) -> dict[str, Any]:
    """Run historical trade plans through an isolated PaperEngine instance."""

    root = Path(project_root)
    got_run_id = str(run_id or f"paper_equiv_{utc_now_iso().replace(':', '').replace('-', '')}")
    provider = HistoricalCandleProvider(candles_by_symbol)
    paper_config = config or default_paper_equivalent_config(run_id=got_run_id)
    engine = PaperEngine(root, config=paper_config, candle_provider=provider)
    engine.initialize()
    consume = engine.consume_trade_plans(docs)
    ticks: list[dict[str, Any]] = []
    for idx, open_time_ms in enumerate(provider.timeline):
        if max_ticks is not None and idx >= int(max_ticks):
            break
        provider.advance_to(open_time_ms)
        entries = engine.process_pending_entries()
        closes = engine.process_open_positions()
        ticks.append(
            {
                "open_time_ms": open_time_ms,
                "entries": entries,
                "closes": closes,
            }
        )
    summary = engine.store.write_summary()
    counts = _ledger_counts(engine)
    metadata = paper_equivalent_metadata(
        execution_contract_version=EXECUTION_CONTRACT_VERSION,
        paper_adapter_version=PAPER_ADAPTER_VERSION,
        paper_gate_version=PAPER_GATE_VERSION,
        paper_fill_model=paper_config.fill_model_mode,
    )
    result = {
        "schema_version": "step7.145.paper_equivalent_backtest.v1",
        **metadata,
        "paper_equivalent_run_id": got_run_id,
        "db_path": str(engine.store.db_path),
        "summary_path": str(engine.store.summary_path),
        "consume": consume,
        "ticks": ticks,
        "counts": counts,
        "summary": {
            "stats": summary.get("stats"),
            "summary": summary.get("summary"),
        },
    }
    result["trade_quality_completion"] = complete_paper_trade_quality(
        root,
        config=paper_config,
        candle_provider=provider,
    )
    result["training_dataset"] = sync_paper_sqlite_source(
        root,
        source_db_path=engine.store.db_path,
        run_id=f"paper_equivalent_{got_run_id}",
        source_mode="paper_equivalent_backtest",
    )
    result["candidate_ledger"] = sync_candidate_ledger_from_paper_sqlite(
        root,
        source_db_path=engine.store.db_path,
        run_id=f"paper_equivalent_{got_run_id}",
        source_mode="baseline_backtest",
        docs=docs,
        counterfactual_candles_by_symbol={str(symbol).upper(): list(rows) for symbol, rows in candles_by_symbol.items()},
        execution_source="paper_equivalent_backtest",
    )
    return result


def _ledger_counts(engine: PaperEngine) -> dict[str, int]:
    tables = (
        "paper_intent_inbox",
        "paper_skip_ledger",
        "paper_trade_plans",
        "paper_orders",
        "paper_positions",
        "paper_fills",
    )
    out: dict[str, int] = {}
    with engine.store.connect() as conn:
        for table in tables:
            out[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return out


def _as_candle(symbol: str, row: Candle | Mapping[str, Any]) -> Candle:
    if isinstance(row, Candle):
        return row
    return Candle(
        symbol=str(row.get("symbol") or symbol).upper(),
        open_time_ms=int(row.get("open_time_ms") or row.get("open_time") or 0),
        open=float(row.get("open")),
        high=float(row.get("high")),
        low=float(row.get("low")),
        close=float(row.get("close")),
        volume=float(row.get("volume") or 0.0),
    )
