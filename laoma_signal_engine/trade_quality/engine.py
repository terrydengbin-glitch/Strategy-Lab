"""P18 trade quality diagnostics built from the paper SQLite ledger."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.candles import BinanceCandleProvider
from laoma_signal_engine.paper.models import Candle, PaperConfig
from laoma_signal_engine.paper.storage import PaperStore
from laoma_signal_engine.paper.utils import utc_now_iso


SAMPLE_SCHEMA_VERSION = "18.1"
LABEL_SCHEMA_VERSION = "18.2"
AGG_SCHEMA_VERSION = "18.3"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _holding_sec(opened_at: str | None, closed_at: str | None) -> float | None:
    opened = _parse_iso(opened_at)
    closed = _parse_iso(closed_at)
    if opened is None or closed is None:
        return None
    return max(0.0, (closed - opened).total_seconds())


def _bucket_holding(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 5 * 60:
        return "lt_5m"
    if seconds < 15 * 60:
        return "5m_15m"
    if seconds < 60 * 60:
        return "15m_60m"
    if seconds < 2 * 60 * 60:
        return "1h_2h"
    return "gt_2h"


def _sample_id(order: dict[str, Any]) -> str:
    raw = "|".join(
        [
            str(order.get("id") or ""),
            str(order.get("strategy_line") or ""),
            str(order.get("source_plan_hash") or ""),
            SAMPLE_SCHEMA_VERSION,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class TradeQualitySample:
    sample_id: str
    order_id: str
    strategy_line: str
    symbol: str
    side: str
    source_run_id: str | None
    source_cycle_id: str | None
    source_plan_hash: str | None
    opened_at: str | None
    closed_at: str | None
    exit_reason: str
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    quantity: float
    initial_risk_usdt: float
    gross_pnl_usdt: float
    net_pnl_usdt: float
    fee_usdt: float
    slippage_usdt: float
    cost_ratio_R: float | None
    planned_RR: float | None
    net_R: float | None
    MFE_R: float | None
    MAE_R: float | None
    holding_sec: float | None
    holding_bucket: str
    excursion_model: str
    root_cause_label: str
    root_cause_confidence: float
    root_cause_evidence: dict[str, Any]
    secondary_labels: list[str]
    needs_manual_review: bool

    def as_row(self) -> dict[str, Any]:
        row = self.__dict__.copy()
        row["root_cause_evidence_json"] = _json(row.pop("root_cause_evidence"))
        row["secondary_labels_json"] = _json(row.pop("secondary_labels"))
        row["needs_manual_review"] = 1 if self.needs_manual_review else 0
        return row


def ensure_trade_quality_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_quality_samples (
              sample_id TEXT PRIMARY KEY,
              order_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              source_run_id TEXT,
              source_cycle_id TEXT,
              source_plan_hash TEXT,
              opened_at TEXT,
              closed_at TEXT,
              exit_reason TEXT NOT NULL,
              entry_price REAL NOT NULL,
              exit_price REAL NOT NULL,
              stop_loss REAL NOT NULL,
              take_profit REAL NOT NULL,
              quantity REAL NOT NULL,
              initial_risk_usdt REAL NOT NULL,
              gross_pnl_usdt REAL NOT NULL,
              net_pnl_usdt REAL NOT NULL,
              fee_usdt REAL NOT NULL,
              slippage_usdt REAL NOT NULL,
              cost_ratio_R REAL,
              planned_RR REAL,
              net_R REAL,
              MFE_R REAL,
              MAE_R REAL,
              holding_sec REAL,
              holding_bucket TEXT NOT NULL,
              excursion_model TEXT NOT NULL,
              root_cause_label TEXT NOT NULL,
              root_cause_confidence REAL NOT NULL,
              root_cause_evidence_json TEXT NOT NULL,
              secondary_labels_json TEXT NOT NULL,
              needs_manual_review INTEGER NOT NULL,
              sample_schema_version TEXT NOT NULL DEFAULT '18.1',
              label_schema_version TEXT NOT NULL DEFAULT '18.2',
              generated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_quality_aggregates (
              aggregate_id TEXT PRIMARY KEY,
              dimension TEXT NOT NULL,
              key TEXT NOT NULL,
              sample_count INTEGER NOT NULL,
              total_R REAL NOT NULL,
              avg_R REAL,
              win_rate REAL,
              total_net_pnl_usdt REAL NOT NULL,
              evidence_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL,
              UNIQUE(dimension, key)
            );

            CREATE TABLE IF NOT EXISTS trade_quality_recommendations (
              action_id TEXT PRIMARY KEY,
              priority TEXT NOT NULL,
              problem TEXT NOT NULL,
              evidence_json TEXT NOT NULL,
              affected_scope TEXT NOT NULL,
              suggested_change TEXT NOT NULL,
              expected_effect TEXT NOT NULL,
              requires_followup_task INTEGER NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_trade_quality_samples_line_symbol
              ON trade_quality_samples(strategy_line, symbol, closed_at);
            CREATE INDEX IF NOT EXISTS idx_trade_quality_samples_root_cause
              ON trade_quality_samples(root_cause_label, closed_at);
            """
        )


class TradeQualityAnalyzer:
    def __init__(
        self,
        project_root: Path,
        *,
        config: PaperConfig | None = None,
        candle_provider: Any | None = None,
    ) -> None:
        self.project_root = project_root.resolve()
        self.config = config or PaperConfig()
        self.store = PaperStore(self.project_root, self.config)
        self.db_path = self.store.db_path
        self.candle_provider = candle_provider or BinanceCandleProvider()

    def analyze(self, *, persist: bool = True, limit: int | None = None) -> dict[str, Any]:
        self.store.initialize()
        samples = self.build_samples(limit=limit)
        aggregates = build_aggregates(samples)
        recommendations = build_recommendations(samples, aggregates)
        if persist:
            self.persist(samples, aggregates, recommendations)
        return {
            "schema_version": AGG_SCHEMA_VERSION,
            "generated_at": utc_now_iso(),
            "db_path": str(self.db_path),
            "sample_count": len(samples),
            "samples": [sample.as_row() for sample in samples],
            "aggregates": aggregates,
            "recommendations": recommendations,
        }

    def build_samples(self, *, limit: int | None = None) -> list[TradeQualitySample]:
        with self.store.connect() as conn:
            sql = "SELECT * FROM paper_orders WHERE status='closed' ORDER BY COALESCE(closed_at, updated_at, created_at) DESC"
            params: list[Any] = []
            if limit is not None:
                sql += " LIMIT ?"
                params.append(int(limit))
            orders = [dict(row) for row in conn.execute(sql, params).fetchall()]
            fills_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
            if orders:
                placeholders = ",".join("?" for _ in orders)
                fill_rows = conn.execute(
                    f"SELECT * FROM paper_fills WHERE order_id IN ({placeholders}) ORDER BY COALESCE(filled_at, '') ASC",
                    [row["id"] for row in orders],
                ).fetchall()
                for row in fill_rows:
                    fills_by_order[str(row["order_id"])].append(dict(row))
        return [self._sample_from_order(order, fills_by_order.get(str(order["id"]), [])) for order in orders]

    def _sample_from_order(self, order: dict[str, Any], fills: list[dict[str, Any]]) -> TradeQualitySample:
        side = str(order.get("side") or "").upper()
        qty = _num(order.get("quantity") or order.get("planned_quantity"))
        entry_fill = next((row for row in fills if str(row.get("action") or "").lower() == "entry"), None)
        exit_fill = next((row for row in fills if str(row.get("action") or "").lower() != "entry"), None)
        entry = _num(order.get("filled_entry_price") or order.get("entry_price") or (entry_fill or {}).get("fill_price"))
        exit_price = _num(order.get("exit_price") or (exit_fill or {}).get("fill_price"))
        stop = _num(order.get("stop_loss"))
        tp = _num(order.get("take_profit"))
        initial_risk = abs(entry - stop) * qty if qty > 0 and entry > 0 and stop > 0 else 0.0
        planned_reward = abs(tp - entry) * qty if qty > 0 and entry > 0 and tp > 0 else 0.0
        gross_pnl = sum(_num(row.get("gross_pnl_usdt")) for row in fills if str(row.get("action") or "").lower() != "entry")
        if not gross_pnl:
            gross_pnl = self._gross_from_order(side, entry, exit_price, qty)
        net_pnl = _num(order.get("realized_pnl_usdt"), sum(_num(row.get("net_pnl_usdt")) for row in fills))
        fee = _num(order.get("fee_usdt"), sum(_num(row.get("fee_usdt")) for row in fills))
        slippage = _num(order.get("slippage_usdt"), sum(_num(row.get("slippage_usdt")) for row in fills))
        net_R = (net_pnl / initial_risk) if initial_risk > 0 else None
        planned_RR = (planned_reward / initial_risk) if initial_risk > 0 else None
        cost_ratio = ((fee + abs(slippage)) / initial_risk) if initial_risk > 0 else None
        mfe_r, mae_r, excursion_model = self._mfe_mae(order, initial_risk, entry, qty)
        holding = _holding_sec(order.get("opened_at"), order.get("closed_at"))
        label, confidence, evidence, secondary, manual = label_root_cause(
            {
                "exit_reason": str(order.get("exit_reason") or "").upper(),
                "net_R": net_R,
                "MFE_R": mfe_r,
                "MAE_R": mae_r,
                "planned_RR": planned_RR,
                "cost_ratio_R": cost_ratio,
                "holding_sec": holding,
                "excursion_model": excursion_model,
            }
        )
        return TradeQualitySample(
            sample_id=_sample_id(order),
            order_id=str(order.get("id") or ""),
            strategy_line=str(order.get("strategy_line") or "unknown"),
            symbol=str(order.get("symbol") or "unknown"),
            side=side or "UNKNOWN",
            source_run_id=order.get("source_run_id"),
            source_cycle_id=order.get("source_cycle_id"),
            source_plan_hash=order.get("source_plan_hash"),
            opened_at=order.get("opened_at"),
            closed_at=order.get("closed_at"),
            exit_reason=str(order.get("exit_reason") or "unknown").upper(),
            entry_price=entry,
            exit_price=exit_price,
            stop_loss=stop,
            take_profit=tp,
            quantity=qty,
            initial_risk_usdt=initial_risk,
            gross_pnl_usdt=gross_pnl,
            net_pnl_usdt=net_pnl,
            fee_usdt=fee,
            slippage_usdt=slippage,
            cost_ratio_R=cost_ratio,
            planned_RR=planned_RR,
            net_R=net_R,
            MFE_R=mfe_r,
            MAE_R=mae_r,
            holding_sec=holding,
            holding_bucket=_bucket_holding(holding),
            excursion_model=excursion_model,
            root_cause_label=label,
            root_cause_confidence=confidence,
            root_cause_evidence=evidence,
            secondary_labels=secondary,
            needs_manual_review=manual,
        )

    def _gross_from_order(self, side: str, entry: float, exit_price: float, qty: float) -> float:
        if side == "SHORT":
            return (entry - exit_price) * qty
        return (exit_price - entry) * qty

    def _mfe_mae(self, order: dict[str, Any], initial_risk: float, entry: float, qty: float) -> tuple[float | None, float | None, str]:
        if initial_risk <= 0 or entry <= 0 or qty <= 0:
            return None, None, "invalid_initial_risk"
        candles = self._candles_for_order(order)
        if candles:
            side = str(order.get("side") or "").upper()
            if side == "SHORT":
                favorable = max(max(0.0, entry - _num(candle.low)) * qty for candle in candles)
                adverse = max(max(0.0, _num(candle.high) - entry) * qty for candle in candles)
            else:
                favorable = max(max(0.0, _num(candle.high) - entry) * qty for candle in candles)
                adverse = max(max(0.0, entry - _num(candle.low)) * qty for candle in candles)
            return favorable / initial_risk, adverse / initial_risk, "candle_1m_replay"
        exit_reason = str(order.get("exit_reason") or "").upper()
        planned_RR = abs(_num(order.get("take_profit")) - entry) * qty / initial_risk if _num(order.get("take_profit")) > 0 else None
        if exit_reason == "TP":
            return planned_RR, None, "outcome_proxy_no_candle_replay"
        if exit_reason == "SL":
            return None, 1.0, "outcome_proxy_no_candle_replay"
        return None, None, "no_candle_replay"

    def _candles_for_order(self, order: dict[str, Any]) -> list[Candle]:
        symbol = str(order.get("symbol") or "")
        if not symbol:
            return []
        provider = self.candle_provider
        if hasattr(provider, "get_range_1m"):
            try:
                candles = provider.get_range_1m(symbol, order.get("opened_at"), order.get("closed_at"))
                return list(candles or [])
            except Exception:
                return []
        try:
            candles = list(provider.get_1m(symbol, limit=500) or [])
        except Exception:
            return []
        if not candles:
            return []
        opened = _parse_iso(order.get("opened_at"))
        closed = _parse_iso(order.get("closed_at"))
        if opened is None or closed is None:
            return candles
        start_ms = int(opened.timestamp() * 1000)
        end_ms = int(closed.timestamp() * 1000)
        filtered = [c for c in candles if start_ms <= int(c.open_time_ms) <= end_ms]
        return filtered or candles

    def persist(
        self,
        samples: list[TradeQualitySample],
        aggregates: list[dict[str, Any]],
        recommendations: list[dict[str, Any]],
    ) -> None:
        ensure_trade_quality_tables(self.db_path)
        now = utc_now_iso()
        with sqlite3.connect(self.db_path) as conn:
            for sample in samples:
                row = sample.as_row()
                row["sample_schema_version"] = SAMPLE_SCHEMA_VERSION
                row["label_schema_version"] = LABEL_SCHEMA_VERSION
                row["generated_at"] = now
                conn.execute(
                    """
                    INSERT INTO trade_quality_samples(
                      sample_id, order_id, strategy_line, symbol, side, source_run_id, source_cycle_id,
                      source_plan_hash, opened_at, closed_at, exit_reason, entry_price, exit_price,
                      stop_loss, take_profit, quantity, initial_risk_usdt, gross_pnl_usdt, net_pnl_usdt,
                      fee_usdt, slippage_usdt, cost_ratio_R, planned_RR, net_R, MFE_R, MAE_R,
                      holding_sec, holding_bucket, excursion_model, root_cause_label,
                      root_cause_confidence, root_cause_evidence_json, secondary_labels_json,
                      needs_manual_review, sample_schema_version, label_schema_version, generated_at
                    ) VALUES(
                      :sample_id, :order_id, :strategy_line, :symbol, :side, :source_run_id, :source_cycle_id,
                      :source_plan_hash, :opened_at, :closed_at, :exit_reason, :entry_price, :exit_price,
                      :stop_loss, :take_profit, :quantity, :initial_risk_usdt, :gross_pnl_usdt, :net_pnl_usdt,
                      :fee_usdt, :slippage_usdt, :cost_ratio_R, :planned_RR, :net_R, :MFE_R, :MAE_R,
                      :holding_sec, :holding_bucket, :excursion_model, :root_cause_label,
                      :root_cause_confidence, :root_cause_evidence_json, :secondary_labels_json,
                      :needs_manual_review, :sample_schema_version, :label_schema_version, :generated_at
                    )
                    ON CONFLICT(sample_id) DO UPDATE SET
                      net_R=excluded.net_R,
                      MFE_R=excluded.MFE_R,
                      MAE_R=excluded.MAE_R,
                      root_cause_label=excluded.root_cause_label,
                      root_cause_confidence=excluded.root_cause_confidence,
                      root_cause_evidence_json=excluded.root_cause_evidence_json,
                      secondary_labels_json=excluded.secondary_labels_json,
                      generated_at=excluded.generated_at
                    """,
                    row,
                )
            conn.execute("DELETE FROM trade_quality_aggregates")
            for item in aggregates:
                conn.execute(
                    """
                    INSERT INTO trade_quality_aggregates(
                      aggregate_id, dimension, key, sample_count, total_R, avg_R, win_rate,
                      total_net_pnl_usdt, evidence_json, schema_version, generated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["aggregate_id"],
                        item["dimension"],
                        item["key"],
                        item["sample_count"],
                        item["total_R"],
                        item.get("avg_R"),
                        item.get("win_rate"),
                        item["total_net_pnl_usdt"],
                        _json(item.get("evidence") or {}),
                        AGG_SCHEMA_VERSION,
                        now,
                    ),
                )
            conn.execute("DELETE FROM trade_quality_recommendations")
            for item in recommendations:
                conn.execute(
                    """
                    INSERT INTO trade_quality_recommendations(
                      action_id, priority, problem, evidence_json, affected_scope,
                      suggested_change, expected_effect, requires_followup_task,
                      schema_version, generated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["action_id"],
                        item["priority"],
                        item["problem"],
                        _json(item.get("evidence") or {}),
                        item["affected_scope"],
                        item["suggested_change"],
                        item["expected_effect"],
                        1 if item.get("requires_followup_task") else 0,
                        AGG_SCHEMA_VERSION,
                        now,
                    ),
                )


def label_root_cause(metrics: dict[str, Any]) -> tuple[str, float, dict[str, Any], list[str], bool]:
    exit_reason = str(metrics.get("exit_reason") or "").upper()
    net_R = metrics.get("net_R")
    mfe = metrics.get("MFE_R")
    mae = metrics.get("MAE_R")
    planned_rr = metrics.get("planned_RR")
    cost_ratio = metrics.get("cost_ratio_R")
    evidence = {
        "exit_reason": exit_reason,
        "net_R": net_R,
        "MFE_R": mfe,
        "MAE_R": mae,
        "planned_RR": planned_rr,
        "cost_ratio_R": cost_ratio,
        "holding_sec": metrics.get("holding_sec"),
        "excursion_model": metrics.get("excursion_model"),
    }
    secondary: list[str] = []
    if cost_ratio is not None and cost_ratio >= 0.25:
        secondary.append("cost_too_high")
    if net_R is None:
        return "unknown_needs_review", 0.2, evidence, secondary, True
    if exit_reason == "TP" or net_R >= 0.8:
        return "tp_hit_good_trade", 0.9, evidence, secondary, False
    if cost_ratio is not None and cost_ratio >= 0.35:
        return "cost_too_high", 0.8, evidence, secondary, False
    if mfe is not None and mae is not None:
        if mfe < 0.3 and net_R <= 0:
            return "signal_no_edge", 0.8, evidence, secondary, False
        if mfe >= 0.8 and exit_reason == "SL":
            return "stop_too_tight", 0.75, evidence, secondary, False
        if mfe >= 0.8 and net_R <= 0:
            return "exit_too_late", 0.7, evidence, secondary, False
        if planned_rr is not None and mfe >= 0.5 and mfe < planned_rr and net_R <= 0:
            return "tp_too_far", 0.65, evidence, secondary, False
        if mae >= 0.8 and mfe < 0.5 and net_R <= 0:
            return "entered_too_early", 0.65, evidence, secondary, False
    if exit_reason == "SL":
        return "direction_wrong", 0.45, evidence, secondary, True
    if net_R < 0:
        return "unknown_needs_review", 0.35, evidence, secondary, True
    return "profitable_unclassified", 0.4, evidence, secondary, True


def build_aggregates(samples: list[TradeQualitySample]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[TradeQualitySample]] = defaultdict(list)
    for sample in samples:
        values = {
            "strategy_line": sample.strategy_line,
            "side": sample.side,
            "symbol": sample.symbol,
            "exit_reason": sample.exit_reason,
            "root_cause": sample.root_cause_label,
            "holding_bucket": sample.holding_bucket,
        }
        for dimension, key in values.items():
            groups[(dimension, key)].append(sample)
    items: list[dict[str, Any]] = []
    for (dimension, key), rows in sorted(groups.items()):
        r_values = [row.net_R for row in rows if row.net_R is not None]
        total_r = sum(r_values)
        wins = [row for row in rows if (row.net_R or 0) > 0]
        total_pnl = sum(row.net_pnl_usdt for row in rows)
        aggregate_id = hashlib.sha256(f"{dimension}|{key}|{AGG_SCHEMA_VERSION}".encode("utf-8")).hexdigest()[:24]
        items.append(
            {
                "aggregate_id": aggregate_id,
                "dimension": dimension,
                "key": key,
                "sample_count": len(rows),
                "total_R": round(total_r, 8),
                "avg_R": round(total_r / len(r_values), 8) if r_values else None,
                "win_rate": round(len(wins) / len(rows), 6) if rows else None,
                "total_net_pnl_usdt": round(total_pnl, 8),
                "evidence": {
                    "sample_ids": [row.sample_id for row in rows[:25]],
                    "root_cause_counts": dict(Counter(row.root_cause_label for row in rows)),
                },
            }
        )
    return items


def build_recommendations(samples: list[TradeQualitySample], aggregates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    recommendations: list[dict[str, Any]] = []
    if not samples:
        return recommendations
    root_causes = Counter(sample.root_cause_label for sample in samples)
    total_r_by_cause: dict[str, float] = defaultdict(float)
    for sample in samples:
        total_r_by_cause[sample.root_cause_label] += sample.net_R or 0.0
    worst = sorted(total_r_by_cause.items(), key=lambda row: row[1])[:3]
    for cause, total_r in worst:
        count = root_causes[cause]
        if count < 2 and total_r > -1.0:
            continue
        recommendations.append(_recommendation_for_cause(cause, count, total_r, samples))
    unknown = root_causes.get("unknown_needs_review", 0)
    if unknown:
        recommendations.append(
            {
                "action_id": "trade_quality_reduce_unknown_labels",
                "priority": "P1",
                "problem": "部分交易无法用当前 MFE/MAE/R 证据归因",
                "evidence": {"unknown_count": unknown, "sample_count": len(samples)},
                "affected_scope": "paper_closed_trades",
                "suggested_change": "补齐历史 1m candle replay 与 entry/exit timestamp 对齐，降低 proxy 归因比例。",
                "expected_effect": "减少 unknown_needs_review，提高后续参数调整的可信度。",
                "requires_followup_task": True,
            }
        )
    return recommendations


def _recommendation_for_cause(cause: str, count: int, total_r: float, samples: list[TradeQualitySample]) -> dict[str, Any]:
    mapping = {
        "signal_no_edge": ("P0", "信号入场后几乎没有顺向波动", "提高进入 trade plan executable 前的动量/微结构确认门槛。"),
        "direction_wrong": ("P0", "SL 类亏损占比较高且缺少顺向证据", "复查 direction gate 与 long/short 对称校准，避免弱方向进入。"),
        "entered_too_early": ("P1", "入场后先承受较大反向波动", "引入 pullback/confirmation entry 或放慢 market entry。"),
        "stop_too_tight": ("P1", "曾经接近盈利但被止损打掉", "按波动/ATR 或结构位校准 SL 宽度。"),
        "tp_too_far": ("P1", "MFE 不足以触达计划 TP", "降低单 TP 目标或按分层质量使用不同 RR。"),
        "exit_too_late": ("P1", "有顺向机会但最终亏损", "增加时间止盈/回撤退出观察。"),
        "cost_too_high": ("P0", "手续费和滑点占初始风险过高", "过滤低价差/薄流动性 symbol，或提高最小风险空间。"),
    }
    priority, problem, suggested = mapping.get(cause, ("P2", f"{cause} 聚类亏损", "建立专项复盘任务。"))
    affected = sorted({f"{s.strategy_line}:{s.side}" for s in samples if s.root_cause_label == cause})[:10]
    return {
        "action_id": f"trade_quality_{cause}",
        "priority": priority,
        "problem": problem,
        "evidence": {"root_cause": cause, "sample_count": count, "total_R": round(total_r, 8)},
        "affected_scope": ",".join(affected) or "paper_closed_trades",
        "suggested_change": suggested,
        "expected_effect": "减少该类亏损聚类，提高平均 R 和交易质量稳定性。",
        "requires_followup_task": True,
    }


def analyze_paper_trades(
    project_root: Path,
    *,
    config: PaperConfig | None = None,
    candle_provider: Any | None = None,
    persist: bool = True,
    limit: int | None = None,
) -> dict[str, Any]:
    return TradeQualityAnalyzer(project_root, config=config, candle_provider=candle_provider).analyze(persist=persist, limit=limit)
