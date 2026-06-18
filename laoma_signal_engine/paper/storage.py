"""SQLite state layer for P14 paper trading."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.models import ACTIVE_ORDER_STATUSES, OPEN_POSITION_STATUSES, STRATEGY_LINES, PaperConfig, PaperIntent
from laoma_signal_engine.paper.utils import atomic_write_json, json_dumps, json_loads, new_id, utc_now_iso
from laoma_signal_engine.research_db import upsert_paper_order_native


SCHEMA_VERSION = "14.3A"


class PaperStore:
    def __init__(self, project_root: Path, config: PaperConfig | None = None) -> None:
        self.project_root = project_root.resolve()
        self.config = config or PaperConfig()
        self.db_path = self._resolve(self.config.db_path)
        self.summary_path = self._resolve(self.config.summary_path)

    def _resolve(self, path: str) -> Path:
        got = Path(path)
        return got if got.is_absolute() else self.project_root / got

    def connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS paper_accounts (
                  strategy_line TEXT PRIMARY KEY,
                  initial_equity_usdt REAL NOT NULL,
                  equity_usdt REAL NOT NULL,
                  realized_pnl_usdt REAL NOT NULL DEFAULT 0,
                  unrealized_pnl_usdt REAL NOT NULL DEFAULT 0,
                  fee_usdt REAL NOT NULL DEFAULT 0,
                  slippage_usdt REAL NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_trade_plans (
                  id TEXT PRIMARY KEY,
                  strategy_line TEXT NOT NULL,
                  source TEXT NOT NULL,
                  source_path TEXT NOT NULL,
                  source_run_id TEXT,
                  source_cycle_id TEXT,
                  source_generated_at TEXT,
                  source_plan_hash TEXT NOT NULL,
                  intent_id TEXT,
                  reset_epoch_id TEXT,
                  consumed_at TEXT,
                  signal_class TEXT NOT NULL DEFAULT 'executable',
                  paper_eligible INTEGER NOT NULL DEFAULT 1,
                  notify_eligible INTEGER NOT NULL DEFAULT 0,
                  source_executable INTEGER NOT NULL DEFAULT 0,
                  source_action TEXT,
                  source_entry_mode TEXT,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  intent_type TEXT NOT NULL,
                  opportunity_type TEXT NOT NULL,
                  order_type TEXT NOT NULL,
                  entry_price REAL NOT NULL,
                  stop_loss REAL NOT NULL,
                  take_profit REAL NOT NULL,
                  tp1 REAL,
                  margin_usdt REAL NOT NULL,
                  leverage REAL NOT NULL,
                  sizing_method TEXT,
                  risk_budget_usdt REAL,
                  planned_quantity REAL,
                  planned_notional_usdt REAL,
                  estimated_max_loss_usdt REAL,
                  status TEXT NOT NULL,
                  reason_codes_json TEXT NOT NULL,
                  guards_json TEXT NOT NULL,
                  source_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  UNIQUE(strategy_line, source_plan_hash)
                );

                CREATE TABLE IF NOT EXISTS paper_orders (
                  id TEXT PRIMARY KEY,
                  plan_id TEXT,
                  strategy_line TEXT NOT NULL,
                  source_run_id TEXT,
                  source_cycle_id TEXT,
                  source_plan_hash TEXT,
                  intent_id TEXT,
                  reset_epoch_id TEXT,
                  consumed_at TEXT,
                  signal_class TEXT NOT NULL DEFAULT 'executable',
                  paper_eligible INTEGER NOT NULL DEFAULT 1,
                  notify_eligible INTEGER NOT NULL DEFAULT 0,
                  source_executable INTEGER NOT NULL DEFAULT 0,
                  source_action TEXT,
                  source_entry_mode TEXT,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  status TEXT NOT NULL,
                  order_type TEXT NOT NULL,
                  entry_price REAL NOT NULL,
                  filled_entry_price REAL,
                  stop_loss REAL NOT NULL,
                  take_profit REAL NOT NULL,
                  tp1 REAL,
                  margin_usdt REAL NOT NULL,
                  leverage REAL NOT NULL,
                  sizing_method TEXT,
                  risk_budget_usdt REAL,
                  planned_quantity REAL,
                  planned_notional_usdt REAL,
                  estimated_max_loss_usdt REAL,
                  quantity REAL NOT NULL DEFAULT 0,
                  remaining_quantity REAL NOT NULL DEFAULT 0,
                  notional_usdt REAL NOT NULL DEFAULT 0,
                  reference_price REAL,
                  slippage_bps REAL NOT NULL DEFAULT 0,
                  slippage_usdt REAL NOT NULL DEFAULT 0,
                  fee_bps REAL NOT NULL DEFAULT 0,
                  fee_usdt REAL NOT NULL DEFAULT 0,
                  realized_pnl_usdt REAL NOT NULL DEFAULT 0,
                  unrealized_pnl_usdt REAL NOT NULL DEFAULT 0,
                  exit_price REAL,
                  exit_reason TEXT,
                  opened_at TEXT,
                  closed_at TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_positions (
                  id TEXT PRIMARY KEY,
                  order_id TEXT NOT NULL,
                  strategy_line TEXT NOT NULL,
                  source_run_id TEXT,
                  source_cycle_id TEXT,
                  source_plan_hash TEXT,
                  intent_id TEXT,
                  reset_epoch_id TEXT,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  status TEXT NOT NULL,
                  entry_price REAL NOT NULL,
                  quantity REAL NOT NULL,
                  remaining_quantity REAL NOT NULL,
                  notional_usdt REAL NOT NULL,
                  margin_usdt REAL NOT NULL,
                  leverage REAL NOT NULL,
                  sizing_method TEXT,
                  risk_budget_usdt REAL,
                  planned_quantity REAL,
                  planned_notional_usdt REAL,
                  estimated_max_loss_usdt REAL,
                  stop_loss REAL NOT NULL,
                  take_profit REAL NOT NULL,
                  tp1 REAL,
                  realized_pnl_usdt REAL NOT NULL DEFAULT 0,
                  unrealized_pnl_usdt REAL NOT NULL DEFAULT 0,
                  fee_usdt REAL NOT NULL DEFAULT 0,
                  slippage_usdt REAL NOT NULL DEFAULT 0,
                  opened_at TEXT NOT NULL,
                  closed_at TEXT,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_fills (
                  id TEXT PRIMARY KEY,
                  order_id TEXT NOT NULL,
                  position_id TEXT,
                  strategy_line TEXT NOT NULL,
                  source_run_id TEXT,
                  source_cycle_id TEXT,
                  source_plan_hash TEXT,
                  intent_id TEXT,
                  reset_epoch_id TEXT,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  action TEXT NOT NULL,
                  reference_price REAL NOT NULL,
                  fill_price REAL NOT NULL,
                  quantity REAL NOT NULL,
                  notional_usdt REAL NOT NULL,
                  fee_bps REAL NOT NULL DEFAULT 0,
                  fee_usdt REAL NOT NULL DEFAULT 0,
                  slippage_bps REAL NOT NULL DEFAULT 0,
                  slippage_usdt REAL NOT NULL DEFAULT 0,
                  gross_pnl_usdt REAL NOT NULL DEFAULT 0,
                  net_pnl_usdt REAL NOT NULL DEFAULT 0,
                  candle_open_time_ms INTEGER,
                  planned_entry_price REAL,
                  entry_drift_bps REAL,
                  fill_delay_sec REAL,
                  fill_model TEXT,
                  cost_source TEXT,
                  slippage_source TEXT,
                  liquidity_penalty_bps REAL,
                  volatility_penalty_bps REAL,
                  same_candle_policy TEXT,
                  source_generated_at TEXT,
                  consumed_at TEXT,
                  filled_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_consumed_plans (
                  strategy_line TEXT NOT NULL,
                  source_plan_hash TEXT NOT NULL,
                  order_id TEXT,
                  consumed_at TEXT NOT NULL,
                  PRIMARY KEY(strategy_line, source_plan_hash)
                );

                CREATE TABLE IF NOT EXISTS paper_skip_ledger (
                  id TEXT PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  source_run_id TEXT,
                  source_cycle_id TEXT,
                  strategy_line TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  source_plan_hash TEXT NOT NULL,
                  source_path TEXT,
                  source_generated_at TEXT,
                  executable INTEGER NOT NULL DEFAULT 0,
                  paper_eligible INTEGER NOT NULL DEFAULT 0,
                  skip_reason TEXT NOT NULL,
                  skip_detail_json TEXT NOT NULL,
                  source_json TEXT NOT NULL,
                  UNIQUE(strategy_line, source_plan_hash)
                );

                CREATE TABLE IF NOT EXISTS paper_performance_snapshots (
                  id TEXT PRIMARY KEY,
                  strategy_line TEXT NOT NULL,
                  stats_json TEXT NOT NULL,
                  snapshot_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_worker_status (
                  key TEXT PRIMARY KEY,
                  value_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_reset_epochs (
                  strategy_line TEXT PRIMARY KEY,
                  reset_epoch_id TEXT NOT NULL,
                  experiment_id TEXT,
                  reset_at TEXT NOT NULL,
                  reset_after_run_id TEXT,
                  reason TEXT NOT NULL,
                  detail_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS paper_intent_inbox (
                  intent_id TEXT PRIMARY KEY,
                  strategy_line TEXT NOT NULL,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  source_run_id TEXT,
                  source_cycle_id TEXT,
                  source_generated_at TEXT,
                  source_plan_hash TEXT NOT NULL,
                  source_path TEXT,
                  source_json TEXT NOT NULL,
                  status TEXT NOT NULL,
                  skip_reason TEXT,
                  skip_detail_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  consumed_at TEXT,
                  updated_at TEXT NOT NULL,
                  UNIQUE(strategy_line, source_plan_hash)
                );

                CREATE INDEX IF NOT EXISTS idx_paper_orders_line_symbol_status
                  ON paper_orders(strategy_line, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_paper_positions_line_symbol_status
                  ON paper_positions(strategy_line, symbol, status);
                CREATE INDEX IF NOT EXISTS idx_paper_fills_line_symbol_time
                  ON paper_fills(strategy_line, symbol, filled_at);
                CREATE INDEX IF NOT EXISTS idx_paper_consumed_line_hash
                  ON paper_consumed_plans(strategy_line, source_plan_hash);
                CREATE INDEX IF NOT EXISTS idx_paper_skip_line_hash
                  ON paper_skip_ledger(strategy_line, source_plan_hash);
                CREATE INDEX IF NOT EXISTS idx_paper_skip_run_line
                  ON paper_skip_ledger(source_run_id, strategy_line);
                CREATE INDEX IF NOT EXISTS idx_paper_perf_line_time
                  ON paper_performance_snapshots(strategy_line, snapshot_at);
                CREATE INDEX IF NOT EXISTS idx_paper_intent_line_status
                  ON paper_intent_inbox(strategy_line, status, created_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_positions_order_once
                  ON paper_positions(order_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_fills_entry_once
                  ON paper_fills(order_id, action)
                  WHERE action = 'entry';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_fills_exit_once
                  ON paper_fills(position_id, action)
                  WHERE action != 'entry' AND position_id IS NOT NULL;
                """
            )
            self._ensure_column(conn, "paper_trade_plans", "signal_class", "TEXT NOT NULL DEFAULT 'executable'")
            self._ensure_column(conn, "paper_trade_plans", "paper_eligible", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(conn, "paper_trade_plans", "notify_eligible", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "paper_trade_plans", "source_executable", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "paper_trade_plans", "source_action", "TEXT")
            self._ensure_column(conn, "paper_trade_plans", "source_entry_mode", "TEXT")
            self._ensure_column(conn, "paper_orders", "signal_class", "TEXT NOT NULL DEFAULT 'executable'")
            self._ensure_column(conn, "paper_orders", "paper_eligible", "INTEGER NOT NULL DEFAULT 1")
            self._ensure_column(conn, "paper_orders", "notify_eligible", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "paper_orders", "source_executable", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "paper_orders", "source_action", "TEXT")
            self._ensure_column(conn, "paper_orders", "source_entry_mode", "TEXT")
            for table in ("paper_trade_plans", "paper_orders", "paper_positions"):
                self._ensure_column(conn, table, "sizing_method", "TEXT")
                self._ensure_column(conn, table, "risk_budget_usdt", "REAL")
                self._ensure_column(conn, table, "planned_quantity", "REAL")
                self._ensure_column(conn, table, "planned_notional_usdt", "REAL")
                self._ensure_column(conn, table, "estimated_max_loss_usdt", "REAL")
            for table in ("paper_trade_plans", "paper_orders"):
                self._ensure_column(conn, table, "intent_id", "TEXT")
                self._ensure_column(conn, table, "reset_epoch_id", "TEXT")
                self._ensure_column(conn, table, "consumed_at", "TEXT")
                self._ensure_column(conn, table, "experiment_id", "TEXT")
                self._ensure_column(conn, table, "gate_candidate_id", "TEXT")
                self._ensure_column(conn, table, "gate_decision", "TEXT")
                self._ensure_column(conn, table, "gate_rule_json", "TEXT")
                self._ensure_column(conn, table, "gate_features_json", "TEXT")
            for table in ("paper_skip_ledger", "paper_intent_inbox"):
                self._ensure_column(conn, table, "experiment_id", "TEXT")
                self._ensure_column(conn, table, "gate_candidate_id", "TEXT")
                self._ensure_column(conn, table, "gate_decision", "TEXT")
                self._ensure_column(conn, table, "gate_rule_json", "TEXT")
                self._ensure_column(conn, table, "gate_features_json", "TEXT")
            for table in ("paper_positions", "paper_fills"):
                self._ensure_column(conn, table, "intent_id", "TEXT")
                self._ensure_column(conn, table, "reset_epoch_id", "TEXT")
            for table in ("paper_orders", "paper_positions", "paper_fills"):
                self._ensure_column(conn, table, "planned_entry_price", "REAL")
                self._ensure_column(conn, table, "entry_drift_bps", "REAL")
                self._ensure_column(conn, table, "fill_delay_sec", "REAL")
                self._ensure_column(conn, table, "fill_model", "TEXT")
                self._ensure_column(conn, table, "cost_source", "TEXT")
                self._ensure_column(conn, table, "slippage_source", "TEXT")
                self._ensure_column(conn, table, "liquidity_penalty_bps", "REAL")
                self._ensure_column(conn, table, "volatility_penalty_bps", "REAL")
                self._ensure_column(conn, table, "same_candle_policy", "TEXT")
            self._ensure_column(conn, "paper_fills", "source_generated_at", "TEXT")
            self._ensure_column(conn, "paper_fills", "consumed_at", "TEXT")
            now = utc_now_iso()
            for line in STRATEGY_LINES:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO paper_accounts(
                      strategy_line, initial_equity_usdt, equity_usdt, updated_at
                    ) VALUES(?, ?, ?, ?)
                    """,
                    (line, self.config.default_account_equity_usdt, self.config.default_account_equity_usdt, now),
                )

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, spec: str) -> None:
        existing = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {spec}")

    def row_dicts(self, table: str, *, line: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if table not in {
            "paper_accounts",
            "paper_trade_plans",
            "paper_orders",
            "paper_positions",
            "paper_fills",
            "paper_skip_ledger",
            "paper_performance_snapshots",
            "paper_reset_epochs",
            "paper_intent_inbox",
        }:
            raise ValueError(f"unsupported table: {table}")
        where = ""
        params: list[Any] = []
        if line:
            self.validate_line(line)
            where = " WHERE strategy_line = ?"
            params.append(line)
        sql = f"SELECT * FROM {table}{where} ORDER BY rowid DESC LIMIT ?"
        params.append(int(limit))
        with self.connect() as conn:
            return [self._decode_row(dict(row)) for row in conn.execute(sql, params).fetchall()]

    def ordered_rows(
        self,
        table: str,
        *,
        line: str | None = None,
        status: str | None = None,
        limit: int = 200,
        order: str = "rowid DESC",
    ) -> list[dict[str, Any]]:
        if table not in {"paper_orders", "paper_positions", "paper_fills"}:
            raise ValueError(f"unsupported ordered table: {table}")
        if line:
            self.validate_line(line)
        if table == "paper_orders":
            allowed_orders = {
                "closed_at_desc": "COALESCE(closed_at, updated_at, created_at) DESC, rowid DESC",
                "opened_at_desc": "COALESCE(opened_at, updated_at, created_at) DESC, rowid DESC",
                "updated_at_desc": "COALESCE(updated_at, created_at) DESC, rowid DESC",
                "rowid_desc": "rowid DESC",
            }
        elif table == "paper_positions":
            allowed_orders = {
                "closed_at_desc": "COALESCE(closed_at, updated_at, opened_at) DESC, rowid DESC",
                "opened_at_desc": "COALESCE(opened_at, updated_at) DESC, rowid DESC",
                "updated_at_desc": "COALESCE(updated_at, opened_at) DESC, rowid DESC",
                "rowid_desc": "rowid DESC",
            }
        else:
            allowed_orders = {
                "filled_at_desc": "COALESCE(filled_at, '') DESC, rowid DESC",
                "updated_at_desc": "COALESCE(filled_at, '') DESC, rowid DESC",
                "rowid_desc": "rowid DESC",
            }
        order_sql = allowed_orders.get(order, allowed_orders["rowid_desc"])
        clauses: list[str] = []
        params: list[Any] = []
        if line:
            clauses.append("strategy_line = ?")
            params.append(line)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM {table}{where} ORDER BY {order_sql} LIMIT ?"
        params.append(int(limit))
        with self.connect() as conn:
            return [self._decode_row(dict(row)) for row in conn.execute(sql, params).fetchall()]

    def validate_line(self, line: str) -> None:
        if line not in STRATEGY_LINES:
            raise ValueError("invalid_strategy_line")

    def is_consumed(self, intent: PaperIntent) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM paper_consumed_plans WHERE strategy_line = ? AND source_plan_hash = ?",
                (intent.strategy_line, intent.source_plan_hash),
            ).fetchone()
        return row is not None

    def latest_reset_epoch(self, line: str) -> dict[str, Any] | None:
        self.validate_line(line)
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM paper_reset_epochs WHERE strategy_line=?", (line,)).fetchone()
        return self._decode_row(dict(row)) if row else None

    def reset_epochs(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [self._decode_row(dict(row)) for row in conn.execute("SELECT * FROM paper_reset_epochs ORDER BY reset_at DESC").fetchall()]

    def record_reset_epoch(
        self,
        line: str,
        *,
        reset_epoch_id: str,
        experiment_id: str,
        reset_at: str,
        reset_after_run_id: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.validate_line(line)
        payload = {
            "strategy_line": line,
            "reset_epoch_id": reset_epoch_id,
            "experiment_id": experiment_id,
            "reset_at": reset_at,
            "reset_after_run_id": reset_after_run_id,
            "reason": "archive_reset",
            "detail_json": json_dumps(detail or {}),
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO paper_reset_epochs(
                  strategy_line, reset_epoch_id, experiment_id, reset_at, reset_after_run_id, reason, detail_json
                ) VALUES(
                  :strategy_line, :reset_epoch_id, :experiment_id, :reset_at, :reset_after_run_id, :reason, :detail_json
                )
                ON CONFLICT(strategy_line) DO UPDATE SET
                  reset_epoch_id=excluded.reset_epoch_id,
                  experiment_id=excluded.experiment_id,
                  reset_at=excluded.reset_at,
                  reset_after_run_id=excluded.reset_after_run_id,
                  reason=excluded.reason,
                  detail_json=excluded.detail_json
                """,
                payload,
            )
        return {**payload, "detail": detail or {}}

    def enqueue_intent(self, intent: PaperIntent) -> dict[str, Any]:
        self.validate_line(intent.strategy_line)
        now = utc_now_iso()
        intent_id = new_id("paper_intent")
        payload = {
            "intent_id": intent_id,
            "strategy_line": intent.strategy_line,
            "symbol": intent.symbol,
            "side": intent.side,
            "source_run_id": intent.source_run_id,
            "source_cycle_id": intent.source_cycle_id,
            "source_generated_at": intent.source_generated_at,
            "source_plan_hash": intent.source_plan_hash,
            "source_path": intent.source_path,
            "source_json": json_dumps(intent.source_json),
            "status": "pending",
            "skip_reason": None,
            "skip_detail_json": json_dumps({}),
            "created_at": now,
            "consumed_at": None,
            "updated_at": now,
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO paper_intent_inbox(
                  intent_id, strategy_line, symbol, side, source_run_id, source_cycle_id,
                  source_generated_at, source_plan_hash, source_path, source_json, status,
                  skip_reason, skip_detail_json, created_at, consumed_at, updated_at
                ) VALUES(
                  :intent_id, :strategy_line, :symbol, :side, :source_run_id, :source_cycle_id,
                  :source_generated_at, :source_plan_hash, :source_path, :source_json, :status,
                  :skip_reason, :skip_detail_json, :created_at, :consumed_at, :updated_at
                )
                """,
                payload,
            )
            row = conn.execute(
                "SELECT * FROM paper_intent_inbox WHERE strategy_line=? AND source_plan_hash=?",
                (intent.strategy_line, intent.source_plan_hash),
            ).fetchone()
        return self._decode_row(dict(row)) if row else payload

    def mark_intent_status(
        self,
        intent: PaperIntent,
        *,
        status: str,
        skip_reason: str | None = None,
        skip_detail: dict[str, Any] | None = None,
        consumed_at: str | None = None,
    ) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE paper_intent_inbox
                SET status=?, skip_reason=?, skip_detail_json=?, consumed_at=COALESCE(?, consumed_at), updated_at=?
                WHERE strategy_line=? AND source_plan_hash=?
                """,
                (
                    status,
                    skip_reason,
                    json_dumps(skip_detail or {}),
                    consumed_at,
                    now,
                    intent.strategy_line,
                    intent.source_plan_hash,
                ),
            )

    def update_intent_gate_lineage(self, intent: PaperIntent) -> None:
        gate_payload = self._gate_payload_from_source(intent.source_json)
        if not gate_payload:
            return
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE paper_intent_inbox
                SET source_json=?,
                    experiment_id=?,
                    gate_candidate_id=?,
                    gate_decision=?,
                    gate_rule_json=?,
                    gate_features_json=?,
                    updated_at=?
                WHERE strategy_line=? AND source_plan_hash=?
                """,
                (
                    json_dumps(intent.source_json),
                    gate_payload.get("experiment_id"),
                    gate_payload.get("gate_candidate_id"),
                    gate_payload.get("decision"),
                    json_dumps(gate_payload.get("rule_json") or {}),
                    json_dumps(gate_payload.get("features") or {}),
                    now,
                    intent.strategy_line,
                    intent.source_plan_hash,
                ),
            )

    def intent_rows(self, *, line: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if line:
            self.validate_line(line)
        where = " WHERE strategy_line=?" if line else ""
        params: list[Any] = [line] if line else []
        params.append(int(limit))
        with self.connect() as conn:
            rows = conn.execute(f"SELECT * FROM paper_intent_inbox{where} ORDER BY rowid DESC LIMIT ?", params).fetchall()
        return [self._decode_row(dict(row)) for row in rows]

    def active_slot_occupied(self, line: str, symbol: str) -> bool:
        self.validate_line(line)
        order_marks = ",".join("?" for _ in ACTIVE_ORDER_STATUSES)
        pos_marks = ",".join("?" for _ in OPEN_POSITION_STATUSES)
        with self.connect() as conn:
            order = conn.execute(
                f"SELECT 1 FROM paper_orders WHERE strategy_line = ? AND symbol = ? AND status IN ({order_marks}) LIMIT 1",
                (line, symbol.upper(), *ACTIVE_ORDER_STATUSES),
            ).fetchone()
            pos = conn.execute(
                f"SELECT 1 FROM paper_positions WHERE strategy_line = ? AND symbol = ? AND status IN ({pos_marks}) LIMIT 1",
                (line, symbol.upper(), *OPEN_POSITION_STATUSES),
            ).fetchone()
        return order is not None or pos is not None

    def active_slot_snapshot(self, line: str, symbol: str) -> dict[str, Any]:
        self.validate_line(line)
        symbol = symbol.upper()
        order_marks = ",".join("?" for _ in ACTIVE_ORDER_STATUSES)
        pos_marks = ",".join("?" for _ in OPEN_POSITION_STATUSES)
        with self.connect() as conn:
            order = conn.execute(
                f"""
                SELECT id, status, source_run_id, source_plan_hash, created_at
                FROM paper_orders
                WHERE strategy_line = ? AND symbol = ? AND status IN ({order_marks})
                ORDER BY rowid DESC LIMIT 1
                """,
                (line, symbol, *ACTIVE_ORDER_STATUSES),
            ).fetchone()
            pos = conn.execute(
                f"""
                SELECT id, status, source_run_id, source_plan_hash, opened_at
                FROM paper_positions
                WHERE strategy_line = ? AND symbol = ? AND status IN ({pos_marks})
                ORDER BY rowid DESC LIMIT 1
                """,
                (line, symbol, *OPEN_POSITION_STATUSES),
            ).fetchone()
        return {
            "strategy_line": line,
            "symbol": symbol,
            "active_order": dict(order) if order else None,
            "active_position": dict(pos) if pos else None,
        }

    def active_symbol_side_snapshot(
        self,
        symbol: str,
        side: str,
        *,
        lines: tuple[str, ...] = ("without_micro", "strategy4"),
    ) -> dict[str, Any]:
        symbol = symbol.upper()
        side = side.upper()
        for line in lines:
            self.validate_line(line)
        line_marks = ",".join("?" for _ in lines)
        order_marks = ",".join("?" for _ in ACTIVE_ORDER_STATUSES)
        pos_marks = ",".join("?" for _ in OPEN_POSITION_STATUSES)
        with self.connect() as conn:
            order = conn.execute(
                f"""
                SELECT id, strategy_line, status, side, source_run_id, source_plan_hash, created_at
                FROM paper_orders
                WHERE strategy_line IN ({line_marks}) AND symbol = ? AND side = ? AND status IN ({order_marks})
                ORDER BY rowid DESC LIMIT 1
                """,
                (*lines, symbol, side, *ACTIVE_ORDER_STATUSES),
            ).fetchone()
            pos = conn.execute(
                f"""
                SELECT id, strategy_line, status, side, source_run_id, source_plan_hash, opened_at
                FROM paper_positions
                WHERE strategy_line IN ({line_marks}) AND symbol = ? AND side = ? AND status IN ({pos_marks})
                ORDER BY rowid DESC LIMIT 1
                """,
                (*lines, symbol, side, *OPEN_POSITION_STATUSES),
            ).fetchone()
        return {
            "strategy_lines": list(lines),
            "symbol": symbol,
            "side": side,
            "active_order": dict(order) if order else None,
            "active_position": dict(pos) if pos else None,
        }

    def _canonical_skip_reason(self, reason: str) -> str:
        mapping = {
            "source_plan_hash_consumed": "skipped_duplicate_plan_hash",
            "active_slot_occupied": "skipped_same_symbol_open",
            "non_executable": "skipped_not_paper_eligible",
            "paper_eligible_false": "skipped_not_paper_eligible",
            "stale_source": "skipped_stale_source",
            "source_trade_plan_stale_for_paper": "skipped_stale_source",
            "source_trade_plan_before_archive_epoch": "skipped_before_archive_epoch",
            "reentry_cooldown_active": "skipped_reentry_cooldown",
            "reentry_cooldown_after_sl": "skipped_reentry_cooldown",
            "reentry_cooldown_after_forced_close": "skipped_reentry_cooldown",
            "entry_already_processed": "skipped_idempotent_duplicate",
            "position_already_closed": "skipped_idempotent_duplicate",
            "v5_trade_gate_blocked": "skipped_v5_trade_gate_blocked",
            "v5_trade_gate_feature_missing": "skipped_v5_trade_gate_feature_missing",
        }
        return mapping.get(reason, "skipped_adapter_invalid" if reason else "skipped_unknown")

    def _gate_payload_from_source(self, source_json: dict[str, Any]) -> dict[str, Any]:
        got = source_json.get("v5_trade_gate") if isinstance(source_json, dict) else None
        return got if isinstance(got, dict) else {}

    def record_skip(self, row: dict[str, Any], *, reason: str) -> dict[str, Any]:
        line = str(row.get("strategy_line") or row.get("line") or "")
        self.validate_line(line)
        symbol = str(row.get("symbol") or "").upper()
        source_plan_hash = str(row.get("source_plan_hash") or "")
        if not symbol or not source_plan_hash:
            raise ValueError("paper_skip_missing_symbol_or_hash")
        now = utc_now_iso()
        skip_reason = self._canonical_skip_reason(reason)
        detail = dict(row.get("skip_detail") or {})
        detail.setdefault("raw_reason", reason)
        detail.setdefault("reason_codes", list(row.get("reason_codes") or []))
        source_json = row.get("source_json") if isinstance(row.get("source_json"), dict) else dict(row)
        gate_payload = self._gate_payload_from_source(source_json)
        payload = {
            "id": new_id("paper_skip"),
            "created_at": now,
            "source_run_id": row.get("source_run_id"),
            "source_cycle_id": row.get("source_cycle_id"),
            "strategy_line": line,
            "symbol": symbol,
            "source_plan_hash": source_plan_hash,
            "source_path": row.get("source_path") or row.get("source_archive_path"),
            "source_generated_at": row.get("source_generated_at"),
            "executable": int(bool(row.get("source_executable") or row.get("executable"))),
            "paper_eligible": int(bool(row.get("paper_eligible"))),
            "skip_reason": skip_reason,
            "skip_detail_json": json_dumps(detail),
            "source_json": json_dumps(source_json),
            "experiment_id": gate_payload.get("experiment_id"),
            "gate_candidate_id": gate_payload.get("gate_candidate_id"),
            "gate_decision": gate_payload.get("decision"),
            "gate_rule_json": json_dumps(gate_payload.get("rule_json") or {}),
            "gate_features_json": json_dumps(gate_payload.get("features") or {}),
        }
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO paper_skip_ledger(
                  id, created_at, source_run_id, source_cycle_id, strategy_line, symbol,
                  source_plan_hash, source_path, source_generated_at, executable, paper_eligible,
                  skip_reason, skip_detail_json, source_json, experiment_id, gate_candidate_id,
                  gate_decision, gate_rule_json, gate_features_json
                ) VALUES(
                  :id, :created_at, :source_run_id, :source_cycle_id, :strategy_line, :symbol,
                  :source_plan_hash, :source_path, :source_generated_at, :executable, :paper_eligible,
                  :skip_reason, :skip_detail_json, :source_json, :experiment_id, :gate_candidate_id,
                  :gate_decision, :gate_rule_json, :gate_features_json
                )
                """,
                payload,
            )
        return payload

    def last_closed_slot(self, line: str, symbol: str, side: str | None = None) -> dict[str, Any] | None:
        self.validate_line(line)
        clauses = ["strategy_line = ?", "symbol = ?", "status = 'closed'"]
        params: list[Any] = [line, symbol.upper()]
        if side and self.config.reentry_cooldown_scope.endswith("_side"):
            clauses.append("side = ?")
            params.append(side.upper())
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT id, strategy_line, symbol, side, closed_at, exit_reason, source_run_id, source_plan_hash
                FROM paper_orders
                WHERE {" AND ".join(clauses)}
                ORDER BY COALESCE(closed_at, updated_at, created_at) DESC, rowid DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        return dict(row) if row else None

    def create_plan_and_order(
        self,
        intent: PaperIntent,
        *,
        intent_id: str | None = None,
        reset_epoch_id: str | None = None,
    ) -> dict[str, Any]:
        self.validate_line(intent.strategy_line)
        now = utc_now_iso()
        plan_id = new_id("paper_plan")
        order_id = new_id("paper_order")
        gate_payload = self._gate_payload_from_source(intent.source_json)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO paper_trade_plans(
                  id, strategy_line, source, source_path, source_run_id, source_cycle_id,
                  source_generated_at, source_plan_hash, intent_id, reset_epoch_id, consumed_at, signal_class, paper_eligible,
                  notify_eligible, source_executable, source_action, source_entry_mode,
                  symbol, side, intent_type,
                  opportunity_type, order_type, entry_price, stop_loss, take_profit,
                  tp1, margin_usdt, leverage, sizing_method, risk_budget_usdt,
                  planned_quantity, planned_notional_usdt, estimated_max_loss_usdt,
                  status, reason_codes_json, guards_json,
                  source_json, experiment_id, gate_candidate_id, gate_decision, gate_rule_json, gate_features_json, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan_id,
                    intent.strategy_line,
                    intent.source,
                    intent.source_path,
                    intent.source_run_id,
                    intent.source_cycle_id,
                    intent.source_generated_at,
                    intent.source_plan_hash,
                    intent_id,
                    reset_epoch_id,
                    now,
                    intent.signal_class,
                    int(intent.paper_eligible),
                    int(intent.notify_eligible),
                    int(intent.source_executable),
                    intent.source_action,
                    intent.source_entry_mode,
                    intent.symbol,
                    intent.side,
                    intent.intent_type,
                    intent.opportunity_type,
                    intent.order_type,
                    intent.entry_price,
                    intent.stop_loss,
                    intent.take_profit,
                    intent.tp1,
                    intent.margin_usdt,
                    intent.leverage,
                    intent.sizing_method,
                    intent.risk_budget_usdt,
                    intent.planned_quantity,
                    intent.planned_notional_usdt,
                    intent.estimated_max_loss_usdt,
                    "order_created",
                    json_dumps(intent.reason_codes),
                    json_dumps(intent.guards),
                    json_dumps(intent.source_json),
                    gate_payload.get("experiment_id"),
                    gate_payload.get("gate_candidate_id"),
                    gate_payload.get("decision"),
                    json_dumps(gate_payload.get("rule_json") or {}),
                    json_dumps(gate_payload.get("features") or {}),
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO paper_orders(
                  id, plan_id, strategy_line, source_run_id, source_cycle_id, source_plan_hash,
                  intent_id, reset_epoch_id, consumed_at,
                  signal_class, paper_eligible, notify_eligible, source_executable, source_action,
                  source_entry_mode, symbol, side, status, order_type, entry_price, stop_loss, take_profit,
                  tp1, margin_usdt, leverage, sizing_method, risk_budget_usdt,
                  planned_quantity, planned_notional_usdt, estimated_max_loss_usdt,
                  reference_price, experiment_id, gate_candidate_id, gate_decision, gate_rule_json, gate_features_json,
                  created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    plan_id,
                    intent.strategy_line,
                    intent.source_run_id,
                    intent.source_cycle_id,
                    intent.source_plan_hash,
                    intent_id,
                    reset_epoch_id,
                    now,
                    intent.signal_class,
                    int(intent.paper_eligible),
                    int(intent.notify_eligible),
                    int(intent.source_executable),
                    intent.source_action,
                    intent.source_entry_mode,
                    intent.symbol,
                    intent.side,
                    "pending_entry",
                    intent.order_type,
                    intent.entry_price,
                    intent.stop_loss,
                    intent.take_profit,
                    intent.tp1,
                    intent.margin_usdt,
                    intent.leverage,
                    intent.sizing_method,
                    intent.risk_budget_usdt,
                    intent.planned_quantity,
                    intent.planned_notional_usdt,
                    intent.estimated_max_loss_usdt,
                    intent.reference_price,
                    gate_payload.get("experiment_id"),
                    gate_payload.get("gate_candidate_id"),
                    gate_payload.get("decision"),
                    json_dumps(gate_payload.get("rule_json") or {}),
                    json_dumps(gate_payload.get("features") or {}),
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO paper_consumed_plans(strategy_line, source_plan_hash, order_id, consumed_at)
                VALUES(?, ?, ?, ?)
                """,
                (intent.strategy_line, intent.source_plan_hash, order_id, now),
            )
        return {"plan_id": plan_id, "order_id": order_id, "strategy_line": intent.strategy_line, "symbol": intent.symbol}

    def quarantine_non_market_active_orders(self) -> dict[str, Any]:
        """Remove legacy non-market/non-executable artifacts from active paper state."""
        now = utc_now_iso()
        marks = ",".join("?" for _ in ACTIVE_ORDER_STATUSES)
        with self.connect() as conn:
            invalid = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT id FROM paper_orders
                    WHERE status IN ({marks})
                      AND (
                        source_executable != 1
                        OR order_type != 'market'
                        OR source_action != 'ENTER_MARKET'
                        OR source_entry_mode != 'MARKET'
                      )
                    """,
                    ACTIVE_ORDER_STATUSES,
                ).fetchall()
            ]
            ids = [row["id"] for row in invalid]
            if not ids:
                return {"cancelled_orders": 0, "cancelled_positions": 0}
            id_marks = ",".join("?" for _ in ids)
            conn.execute(
                f"""
                UPDATE paper_positions
                SET status='cancelled', remaining_quantity=0, unrealized_pnl_usdt=0,
                    closed_at=COALESCE(closed_at, ?), updated_at=?
                WHERE status='open' AND order_id IN ({id_marks})
                """,
                (now, now, *ids),
            )
            cancelled_positions = conn.execute("SELECT changes() AS c").fetchone()["c"]
            conn.execute(
                f"""
                UPDATE paper_orders
                SET status='cancelled', remaining_quantity=0, unrealized_pnl_usdt=0,
                    exit_reason='LEGACY_NON_MARKET_CANCELLED',
                    closed_at=COALESCE(closed_at, ?), updated_at=?
                WHERE id IN ({id_marks})
                """,
                (now, now, *ids),
            )
            cancelled_orders = conn.execute("SELECT changes() AS c").fetchone()["c"]
        return {"cancelled_orders": int(cancelled_orders), "cancelled_positions": int(cancelled_positions)}

    def open_orders(self) -> list[dict[str, Any]]:
        marks = ",".join("?" for _ in ACTIVE_ORDER_STATUSES)
        with self.connect() as conn:
            rows = []
            for row in conn.execute(
                f"""
                SELECT o.*, p.guards_json AS plan_guards_json, p.source_json AS plan_source_json
                FROM paper_orders o
                LEFT JOIN paper_trade_plans p ON p.id = o.plan_id
                WHERE o.status IN ({marks})
                ORDER BY o.created_at ASC
                """,
                ACTIVE_ORDER_STATUSES,
            ).fetchall():
                got = dict(row)
                got["plan_guards"] = json_loads(got.pop("plan_guards_json", None), {})
                got["plan_source"] = json_loads(got.pop("plan_source_json", None), {})
                rows.append(got)
            return rows

    def open_positions(self) -> list[dict[str, Any]]:
        marks = ",".join("?" for _ in OPEN_POSITION_STATUSES)
        with self.connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    f"SELECT * FROM paper_positions WHERE status IN ({marks}) ORDER BY opened_at ASC",
                    OPEN_POSITION_STATUSES,
                ).fetchall()
            ]

    def execute_entry(self, order: dict[str, Any], cost: Any, *, quantity: float, at: str, candle_ms: int | None) -> str | None:
        position_id = new_id("paper_position")
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE paper_orders
                SET status='open', filled_entry_price=?, quantity=?, remaining_quantity=?,
                    notional_usdt=?, fee_bps=?, fee_usdt=?, slippage_bps=?,
                    slippage_usdt=?, planned_entry_price=?, entry_drift_bps=?,
                    fill_delay_sec=?, fill_model=?, cost_source=?, slippage_source=?,
                    liquidity_penalty_bps=?, volatility_penalty_bps=?, same_candle_policy=?,
                    updated_at=?, opened_at=?
                WHERE id=? AND status='pending_entry'
                """,
                (
                    cost.fill_price,
                    quantity,
                    quantity,
                    cost.notional_usdt,
                    cost.fee_bps,
                    cost.fee_usdt,
                    cost.slippage_bps,
                    cost.slippage_usdt,
                    cost.planned_entry_price,
                    cost.entry_drift_bps,
                    cost.fill_delay_sec,
                    cost.fill_model,
                    cost.cost_source,
                    cost.slippage_source,
                    cost.liquidity_penalty_bps,
                    cost.volatility_penalty_bps,
                    cost.same_candle_policy,
                    now,
                    at,
                    order["id"],
                ),
            )
            changed = conn.execute("SELECT changes() AS c").fetchone()["c"]
            if int(changed or 0) != 1:
                return None
            conn.execute(
                """
                INSERT INTO paper_positions(
                  id, order_id, strategy_line, source_run_id, source_cycle_id, source_plan_hash,
                  intent_id, reset_epoch_id,
                  symbol, side, status, entry_price, quantity, remaining_quantity, notional_usdt,
                  margin_usdt, leverage, sizing_method, risk_budget_usdt, planned_quantity,
                  planned_notional_usdt, estimated_max_loss_usdt,
                  stop_loss, take_profit, tp1, fee_usdt, slippage_usdt,
                  planned_entry_price, entry_drift_bps, fill_delay_sec, fill_model, cost_source,
                  slippage_source, liquidity_penalty_bps, volatility_penalty_bps, same_candle_policy,
                  opened_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    position_id,
                    order["id"],
                    order["strategy_line"],
                    order.get("source_run_id"),
                    order.get("source_cycle_id"),
                    order.get("source_plan_hash"),
                    order.get("intent_id"),
                    order.get("reset_epoch_id"),
                    order["symbol"],
                    order["side"],
                    cost.fill_price,
                    quantity,
                    quantity,
                    cost.notional_usdt,
                    order["margin_usdt"],
                    order["leverage"],
                    order.get("sizing_method"),
                    order.get("risk_budget_usdt"),
                    order.get("planned_quantity"),
                    order.get("planned_notional_usdt"),
                    order.get("estimated_max_loss_usdt"),
                    order["stop_loss"],
                    order["take_profit"],
                    order.get("tp1"),
                    cost.fee_usdt,
                    cost.slippage_usdt,
                    cost.planned_entry_price,
                    cost.entry_drift_bps,
                    cost.fill_delay_sec,
                    cost.fill_model,
                    cost.cost_source,
                    cost.slippage_source,
                    cost.liquidity_penalty_bps,
                    cost.volatility_penalty_bps,
                    cost.same_candle_policy,
                    at,
                    now,
                ),
            )
            self._insert_fill(conn, order, position_id, "entry", cost, quantity, 0.0, -cost.fee_usdt, candle_ms, at)
            conn.execute(
                """
                UPDATE paper_accounts
                SET fee_usdt = fee_usdt + ?,
                    slippage_usdt = slippage_usdt + ?,
                    equity_usdt = equity_usdt - ?,
                    updated_at = ?
                WHERE strategy_line = ?
                """,
                (cost.fee_usdt, cost.slippage_usdt, cost.fee_usdt, now, order["strategy_line"]),
            )
        return position_id

    def close_position(self, position: dict[str, Any], cost: Any, *, gross_pnl: float, exit_reason: str, at: str, candle_ms: int | None) -> bool:
        net_pnl = gross_pnl - cost.fee_usdt
        total_fee = float(position.get("fee_usdt") or 0) + cost.fee_usdt
        total_slippage = float(position.get("slippage_usdt") or 0) + cost.slippage_usdt
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE paper_positions
                SET status='closed', remaining_quantity=0, realized_pnl_usdt=?,
                    unrealized_pnl_usdt=0, fee_usdt=?, slippage_usdt=?,
                    closed_at=?, updated_at=?
                WHERE id=? AND status='open'
                """,
                (net_pnl, total_fee, total_slippage, at, now, position["id"]),
            )
            changed = conn.execute("SELECT changes() AS c").fetchone()["c"]
            if int(changed or 0) != 1:
                return False
            conn.execute(
                """
                UPDATE paper_orders
                SET status='closed', remaining_quantity=0, realized_pnl_usdt=?,
                    unrealized_pnl_usdt=0, exit_price=?, exit_reason=?, fee_usdt=?,
                    slippage_usdt=?, fill_model=?, cost_source=?, slippage_source=?,
                    liquidity_penalty_bps=?, volatility_penalty_bps=?, same_candle_policy=?,
                    closed_at=?, updated_at=?
                WHERE id=?
                """,
                (
                    net_pnl,
                    cost.fill_price,
                    exit_reason,
                    total_fee,
                    total_slippage,
                    cost.fill_model,
                    cost.cost_source,
                    cost.slippage_source,
                    cost.liquidity_penalty_bps,
                    cost.volatility_penalty_bps,
                    cost.same_candle_policy,
                    at,
                    now,
                    position["order_id"],
                ),
            )
            conn.execute(
                """
                UPDATE paper_accounts
                SET realized_pnl_usdt = realized_pnl_usdt + ?,
                    fee_usdt = fee_usdt + ?,
                    slippage_usdt = slippage_usdt + ?,
                    equity_usdt = equity_usdt + ?,
                    updated_at = ?
                WHERE strategy_line = ?
                """,
                (net_pnl, cost.fee_usdt, cost.slippage_usdt, net_pnl, now, position["strategy_line"]),
            )
            self._insert_fill(conn, position, position["id"], exit_reason, cost, float(position["remaining_quantity"]), gross_pnl, net_pnl, candle_ms, at)
            order_row = conn.execute("SELECT * FROM paper_orders WHERE id=?", (position["order_id"],)).fetchone()
            order_payload = self._decode_row(dict(order_row)) if order_row else None
        if order_payload:
            try:
                upsert_paper_order_native(self.project_root, order_payload)
            except Exception:
                pass
        return True

    def update_unrealized(self, position: dict[str, Any], mark_price: float) -> None:
        qty = float(position["remaining_quantity"])
        entry = float(position["entry_price"])
        if position["side"].upper() == "LONG":
            pnl = (mark_price - entry) * qty
        else:
            pnl = (entry - mark_price) * qty
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                "UPDATE paper_positions SET unrealized_pnl_usdt=?, updated_at=? WHERE id=?",
                (pnl, now, position["id"]),
            )
            conn.execute(
                "UPDATE paper_orders SET unrealized_pnl_usdt=?, updated_at=? WHERE id=?",
                (pnl, now, position["order_id"]),
            )

    def set_worker_status(self, status: dict[str, Any]) -> None:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO paper_worker_status(key, value_json, updated_at)
                VALUES('daemon', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
                """,
                (json_dumps(status), now),
            )

    def worker_status(self) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT value_json FROM paper_worker_status WHERE key='daemon'").fetchone()
        return json_loads(row["value_json"], {}) if row else {"status": "stopped"}

    def build_summary(self) -> dict[str, Any]:
        accounts = {row["strategy_line"]: self._decode_row(dict(row)) for row in self.row_dicts("paper_accounts", limit=20)}
        stats = self.stats()
        worker = self.worker_status()
        last_consume = worker.get("last_consume") if isinstance(worker, dict) else {}
        skipped_signals = (last_consume or {}).get("skipped") if isinstance(last_consume, dict) else []
        positions = {line: self.ordered_rows("paper_positions", line=line, limit=100, order="updated_at_desc") for line in STRATEGY_LINES}
        orders = {line: self.ordered_rows("paper_orders", line=line, limit=100, order="updated_at_desc") for line in STRATEGY_LINES}
        open_positions = {
            line: self.ordered_rows("paper_positions", line=line, status="open", limit=100, order="opened_at_desc")
            for line in STRATEGY_LINES
        }
        settled_positions = {
            line: self.ordered_rows("paper_positions", line=line, status="closed", limit=100, order="closed_at_desc")
            for line in STRATEGY_LINES
        }
        closed_orders = {
            line: self.ordered_rows("paper_orders", line=line, status="closed", limit=100, order="closed_at_desc")
            for line in STRATEGY_LINES
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "source": "paper_trading",
            "generated_at": utc_now_iso(),
            "db_path": str(self.db_path),
            "worker": worker,
            "accounts": accounts,
            "stats": stats,
            "summary": {
                "open_positions": self._count_positions(status="open"),
                "pending_orders": self._count_orders(status="pending_entry"),
                "today_fills": len(self.row_dicts("paper_fills", limit=500)),
                "total_pnl_usdt": stats["global"]["net_pnl_usdt"],
                "skipped_signals": len(skipped_signals or []),
            },
            "orders": orders,
            "positions": positions,
            "open_positions": open_positions,
            "settled_positions": settled_positions,
            "closed_orders": closed_orders,
            "skipped_signals": skipped_signals or [],
            "reset_epochs": self.reset_epochs(),
            "intent_inbox": self.intent_rows(limit=200),
            "recent_fills": {
                line: self.ordered_rows("paper_fills", line=line, limit=50, order="filled_at_desc")
                for line in STRATEGY_LINES
            },
        }

    def write_summary(self) -> dict[str, Any]:
        payload = self.build_summary()
        atomic_write_json(self.summary_path, payload)
        return payload

    def stats(self) -> dict[str, Any]:
        by_line = {line: self._stats_for_line(line) for line in STRATEGY_LINES}
        global_stats: dict[str, Any] = {}
        for key in by_line[STRATEGY_LINES[0]]:
            if key in {"win_rate", "loss_rate", "profit_factor", "avg_holding_sec", "avg_net_r", "avg_win_r", "avg_loss_r"}:
                continue
            global_stats[key] = sum(float(row.get(key) or 0) for row in by_line.values())
        closed = int(global_stats.get("total_trades") or 0)
        wins = sum(int(row.get("winning_trades") or 0) for row in by_line.values())
        losses = sum(int(row.get("losing_trades") or 0) for row in by_line.values())
        gross_win = sum(float(row.get("_gross_win") or 0) for row in by_line.values())
        gross_loss = sum(float(row.get("_gross_loss") or 0) for row in by_line.values())
        global_stats["win_rate"] = round(wins / closed * 100, 4) if closed else 0.0
        global_stats["loss_rate"] = round(losses / closed * 100, 4) if closed else 0.0
        global_stats["profit_factor"] = round(gross_win / abs(gross_loss), 4) if gross_loss else (round(gross_win, 4) if gross_win else 0.0)
        global_stats["avg_net_r"] = 0.0
        global_stats["avg_win_r"] = 0.0
        global_stats["avg_loss_r"] = 0.0
        global_stats["avg_holding_sec"] = 0
        for stats in by_line.values():
            stats.pop("_gross_win", None)
            stats.pop("_gross_loss", None)
        return {"global": global_stats, "by_line": by_line, "by_symbol": {}}

    def _stats_for_line(self, line: str) -> dict[str, Any]:
        self.validate_line(line)
        with self.connect() as conn:
            orders = [dict(row) for row in conn.execute("SELECT * FROM paper_orders WHERE strategy_line=?", (line,)).fetchall()]
            fills = [dict(row) for row in conn.execute("SELECT * FROM paper_fills WHERE strategy_line=?", (line,)).fetchall()]
        closed = [row for row in orders if row["status"] == "closed"]
        active = [row for row in orders if row["status"] in ACTIVE_ORDER_STATUSES]
        wins = [row for row in closed if float(row.get("realized_pnl_usdt") or 0) > 0]
        losses = [row for row in closed if float(row.get("realized_pnl_usdt") or 0) < 0]
        breakeven = [row for row in closed if float(row.get("realized_pnl_usdt") or 0) == 0]
        gross_pnl = sum(float(row.get("gross_pnl_usdt") or 0) for row in fills)
        fees = sum(float(row.get("fee_usdt") or 0) for row in fills)
        slippage = sum(float(row.get("slippage_usdt") or 0) for row in fills)
        net_pnl = sum(float(row.get("net_pnl_usdt") or 0) for row in fills)
        entry_fills = [row for row in fills if row.get("action") == "entry"]
        delay_values = [float(row.get("fill_delay_sec") or 0) for row in entry_fills if row.get("fill_delay_sec") is not None]
        drift_values = [float(row.get("entry_drift_bps") or 0) for row in entry_fills if row.get("entry_drift_bps") is not None]
        slippage_bps_values = [float(row.get("slippage_bps") or 0) for row in fills if row.get("slippage_bps") is not None]
        gross_win = sum(float(row.get("realized_pnl_usdt") or 0) for row in wins)
        gross_loss = sum(float(row.get("realized_pnl_usdt") or 0) for row in losses)
        r_values: list[float] = []
        for row in closed:
            denom = float(row.get("estimated_max_loss_usdt") or row.get("risk_budget_usdt") or 0)
            if denom > 0:
                r_values.append(float(row.get("realized_pnl_usdt") or 0) / denom)
        win_r = [r for r in r_values if r > 0]
        loss_r = [r for r in r_values if r < 0]
        avg_risk_usdt_values = [
            float(row.get("estimated_max_loss_usdt") or row.get("risk_budget_usdt") or 0)
            for row in closed
            if float(row.get("estimated_max_loss_usdt") or row.get("risk_budget_usdt") or 0) > 0
        ]
        total = len(closed)
        return {
            "total_orders": len(orders),
            "active_orders": len(active),
            "open_positions": len([row for row in orders if row["status"] == "open"]),
            "pending_orders": len([row for row in orders if row["status"] == "pending_entry"]),
            "closed_orders": len(closed),
            "cancelled_orders": len([row for row in orders if row["status"] == "cancelled"]),
            "rejected_orders": len([row for row in orders if row["status"] == "rejected"]),
            "total_trades": total,
            "winning_trades": len(wins),
            "losing_trades": len(losses),
            "breakeven_trades": len(breakeven),
            "win_rate": round(len(wins) / total * 100, 4) if total else 0.0,
            "loss_rate": round(len(losses) / total * 100, 4) if total else 0.0,
            "gross_pnl_usdt": round(gross_pnl, 8),
            "fee_usdt": round(fees, 8),
            "slippage_usdt": round(slippage, 8),
            "net_pnl_usdt": round(net_pnl, 8),
            "avg_entry_drift_bps": round(sum(drift_values) / len(drift_values), 4) if drift_values else 0.0,
            "avg_slippage_bps": round(sum(slippage_bps_values) / len(slippage_bps_values), 4) if slippage_bps_values else 0.0,
            "avg_fill_delay_sec": round(sum(delay_values) / len(delay_values), 4) if delay_values else 0.0,
            "avg_net_r": round(sum(r_values) / len(r_values), 4) if r_values else 0.0,
            "avg_win_r": round(sum(win_r) / len(win_r), 4) if win_r else 0.0,
            "avg_loss_r": round(sum(loss_r) / len(loss_r), 4) if loss_r else 0.0,
            "expectancy_r": round(sum(r_values) / len(r_values), 4) if r_values else 0.0,
            "avg_risk_usdt": round(sum(avg_risk_usdt_values) / len(avg_risk_usdt_values), 4)
            if avg_risk_usdt_values
            else 0.0,
            "profit_factor": round(gross_win / abs(gross_loss), 4) if gross_loss else (round(gross_win, 4) if gross_win else 0.0),
            "max_drawdown_usdt": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_holding_sec": 0,
            "market_orders": len([row for row in orders if row["order_type"] == "market"]),
            "limit_orders": len([row for row in orders if row["order_type"] == "limit"]),
            "trigger_orders": len([row for row in orders if row["order_type"] == "trigger"]),
            "_gross_win": gross_win,
            "_gross_loss": gross_loss,
        }

    def _count_orders(self, *, status: str) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT count(*) AS c FROM paper_orders WHERE status=?", (status,)).fetchone()
        return int(row["c"] if row else 0)

    def _count_positions(self, *, status: str) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT count(*) AS c FROM paper_positions WHERE status=?", (status,)).fetchone()
        return int(row["c"] if row else 0)

    def _insert_fill(
        self,
        conn: sqlite3.Connection,
        source: dict[str, Any],
        position_id: str | None,
        action: str,
        cost: Any,
        quantity: float,
        gross_pnl: float,
        net_pnl: float,
        candle_ms: int | None,
        at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO paper_fills(
              id, order_id, position_id, strategy_line, source_run_id, source_cycle_id,
              source_plan_hash, intent_id, reset_epoch_id, symbol, side, action, reference_price, fill_price,
              quantity, notional_usdt, fee_bps, fee_usdt, slippage_bps, slippage_usdt,
              gross_pnl_usdt, net_pnl_usdt, candle_open_time_ms,
              planned_entry_price, entry_drift_bps, fill_delay_sec, fill_model, cost_source,
              slippage_source, liquidity_penalty_bps, volatility_penalty_bps, same_candle_policy,
              source_generated_at, consumed_at, filled_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_id("paper_fill"),
                source.get("order_id") or source.get("id"),
                position_id,
                source["strategy_line"],
                source.get("source_run_id"),
                source.get("source_cycle_id"),
                source.get("source_plan_hash"),
                source.get("intent_id"),
                source.get("reset_epoch_id"),
                source["symbol"],
                source["side"],
                action,
                cost.reference_price,
                cost.fill_price,
                quantity,
                cost.notional_usdt,
                cost.fee_bps,
                cost.fee_usdt,
                cost.slippage_bps,
                cost.slippage_usdt,
                gross_pnl,
                net_pnl,
                candle_ms,
                cost.planned_entry_price,
                cost.entry_drift_bps,
                cost.fill_delay_sec,
                cost.fill_model,
                cost.cost_source,
                cost.slippage_source,
                cost.liquidity_penalty_bps,
                cost.volatility_penalty_bps,
                cost.same_candle_policy,
                cost.source_generated_at or source.get("source_generated_at"),
                cost.consumed_at or source.get("consumed_at"),
                at,
            ),
        )

    def _decode_row(self, row: dict[str, Any]) -> dict[str, Any]:
        for key in ("reason_codes_json", "guards_json", "source_json", "stats_json", "value_json", "detail_json", "skip_detail_json"):
            if key in row:
                row[key.removesuffix("_json")] = json_loads(row.pop(key), [] if key == "reason_codes_json" else {})
        return row
