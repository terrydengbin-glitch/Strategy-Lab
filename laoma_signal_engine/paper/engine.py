"""Paper order matching engine."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.adapter import adapt_documents, load_trade_plan_documents
from laoma_signal_engine.paper.candles import BinanceCandleProvider
from laoma_signal_engine.paper.fill_model import adverse_fill_price, build_fill_cost, paper_pnl
from laoma_signal_engine.paper.models import Candle, PaperConfig, PaperIntent
from laoma_signal_engine.paper.storage import PaperStore
from laoma_signal_engine.paper.utils import utc_now_iso
from laoma_signal_engine.paper.v5_gate import annotate_intent_with_gate, evaluate_paper_v5_trade_gate
from laoma_signal_engine.training_snapshot_sync import sync_paper_sqlite_source
from laoma_signal_engine.trade_quality.auto_completion import complete_paper_trade_quality


class PaperEngine:
    def __init__(self, project_root: Path, *, config: PaperConfig | None = None, candle_provider: Any | None = None) -> None:
        self.project_root = project_root.resolve()
        self.config = config or PaperConfig()
        self.store = PaperStore(self.project_root, self.config)
        self.candle_provider = candle_provider or BinanceCandleProvider()

    def initialize(self) -> None:
        self.store.initialize()

    def consume_trade_plans(self, docs: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        self.initialize()
        got_docs = docs if docs is not None else load_trade_plan_documents(self.project_root)
        adapted = adapt_documents(got_docs, config=self.config)
        intents = adapted["intents"]
        created: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = list(adapted.get("skipped") or [])
        for row in skipped:
            if row.get("source_executable") is True:
                self.store.record_skip(row, reason=str(row.get("skip_reason") or row.get("reason") or "adapter_invalid"))
        for intent in intents:
            inbox_row = self.store.enqueue_intent(intent)
            intent_id = str(inbox_row.get("intent_id") or "")
            epoch = self.store.latest_reset_epoch(intent.strategy_line)
            epoch_reason = self._archive_epoch_skip_reason(intent, epoch)
            if epoch_reason:
                row = self._skip_intent(intent, epoch_reason, detail={"reset_epoch": epoch})
                self.store.record_skip(row, reason=epoch_reason)
                self.store.mark_intent_status(intent, status="skipped", skip_reason=epoch_reason, skip_detail=row.get("skip_detail"))
                skipped.append(row)
                continue
            stale_reason, stale_detail = self._stale_skip_reason(intent)
            if stale_reason:
                row = self._skip_intent(intent, stale_reason, detail=stale_detail)
                self.store.record_skip(row, reason=stale_reason)
                self.store.mark_intent_status(intent, status="expired", skip_reason=stale_reason, skip_detail=row.get("skip_detail"))
                skipped.append(row)
                continue
            if self.store.is_consumed(intent):
                row = self._skip_intent(intent, "source_plan_hash_consumed")
                self.store.record_skip(row, reason="source_plan_hash_consumed")
                self.store.mark_intent_status(intent, status="skipped", skip_reason="source_plan_hash_consumed", skip_detail=row.get("skip_detail"))
                skipped.append(row)
                continue
            if self.config.prevent_same_line_symbol_reentry and self.store.active_slot_occupied(intent.strategy_line, intent.symbol):
                row = self._skip_intent(
                    intent,
                    "active_slot_occupied",
                    detail={"active_slot": self.store.active_slot_snapshot(intent.strategy_line, intent.symbol)},
                )
                self.store.record_skip(row, reason="active_slot_occupied")
                self.store.mark_intent_status(intent, status="skipped", skip_reason="active_slot_occupied", skip_detail=row.get("skip_detail"))
                skipped.append(row)
                continue
            if intent.strategy_line == "strategy4":
                cross_slot = self.store.active_symbol_side_snapshot(
                    intent.symbol,
                    intent.side,
                    lines=("without_micro", "micro_fast", "micro_full"),
                )
                if cross_slot.get("active_order") or cross_slot.get("active_position"):
                    self._annotate_strategy4_cross_line_slot(intent, cross_slot)
            cooldown_reason, cooldown_detail = self._reentry_cooldown_skip_reason(intent)
            if cooldown_reason:
                row = self._skip_intent(intent, cooldown_reason, detail=cooldown_detail)
                self.store.record_skip(row, reason=cooldown_reason)
                self.store.mark_intent_status(intent, status="skipped", skip_reason=cooldown_reason, skip_detail=row.get("skip_detail"))
                skipped.append(row)
                continue
            gate_decision = evaluate_paper_v5_trade_gate(self.project_root, intent)
            if gate_decision.enabled:
                annotate_intent_with_gate(intent, gate_decision)
                self.store.update_intent_gate_lineage(intent)
            if gate_decision.action == "block":
                detail = gate_decision.as_dict()
                row = self._skip_intent(intent, gate_decision.reason, detail=detail)
                self.store.record_skip(row, reason=gate_decision.reason)
                self.store.mark_intent_status(intent, status="skipped", skip_reason=gate_decision.reason, skip_detail=row.get("skip_detail"))
                skipped.append(row)
                continue
            order = self.store.create_plan_and_order(
                intent,
                intent_id=intent_id or None,
                reset_epoch_id=str((epoch or {}).get("reset_epoch_id") or "") or None,
            )
            created.append(order)
            self.store.mark_intent_status(intent, status="consumed", consumed_at=utc_now_iso())
        return {"intents": len(intents), "created": len(created), "skipped": skipped, "orders": created}

    def tick(self, docs: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
        self.initialize()
        consume = self.consume_trade_plans(docs)
        quarantine = self.store.quarantine_non_market_active_orders()
        entries = self.process_pending_entries()
        closes = self.process_open_positions()
        status = {
            "status": "running",
            "last_tick_at": utc_now_iso(),
            "tick_interval_sec": self.config.daemon_tick_interval_sec,
            "last_error": None,
            "last_consume": consume,
        }
        self.store.set_worker_status(status)
        summary = self.store.write_summary()
        trade_quality_completion = complete_paper_trade_quality(
            self.project_root,
            config=self.config,
            candle_provider=self.candle_provider,
        )
        training_dataset = sync_paper_sqlite_source(
            self.project_root,
            source_db_path=self.store.db_path,
            run_id=f"paper_tick_{utc_now_iso().replace(':', '').replace('-', '')}",
            source_mode="paper",
        )
        return {
            "consume": consume,
            "quarantine": quarantine,
            "entries": entries,
            "closes": closes,
            "summary": summary,
            "trade_quality_completion": trade_quality_completion,
            "training_dataset": training_dataset,
        }

    def _skip_intent(self, intent: PaperIntent, reason: str, *, detail: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "strategy_line": intent.strategy_line,
            "line": intent.strategy_line,
            "symbol": intent.symbol,
            "side": intent.side,
            "source_run_id": intent.source_run_id,
            "source_cycle_id": intent.source_cycle_id,
            "source_generated_at": intent.source_generated_at,
            "source_path": intent.source_path,
            "source_archive_path": intent.source_path,
            "source_plan_hash": intent.source_plan_hash,
            "source_executable": intent.source_executable,
            "source_action": intent.source_action,
            "source_entry_mode": intent.source_entry_mode,
            "opportunity_type": intent.opportunity_type,
            "slot_key": f"{intent.strategy_line}:{intent.symbol.upper()}",
            "skip_reason": reason,
            "reason": reason,
            "reason_codes": intent.reason_codes,
            "skip_detail": detail or {},
            "source_json": intent.source_json,
        }

    def _annotate_strategy4_cross_line_slot(self, intent: PaperIntent, cross_slot: dict[str, Any]) -> None:
        """Keep Strategy4 independent while preserving cross-line exposure evidence."""
        observed = {
            "cross_line_active_slot_observed": True,
            "slot_scope": "strategy_line",
            "slot_strategy_line": "strategy4",
            "active_slot": cross_slot,
        }
        intent.guards.setdefault("slot_scope", "strategy_line")
        intent.guards.setdefault("slot_strategy_line", "strategy4")
        intent.guards["cross_line_active_slot_observed"] = True
        intent.guards["cross_line_active_slot"] = cross_slot
        intent.source_json.setdefault("slot_scope", "strategy_line")
        intent.source_json.setdefault("slot_strategy_line", "strategy4")
        intent.source_json["cross_line_active_slot_observed"] = True
        intent.source_json["cross_line_active_slot"] = cross_slot
        evidence = intent.source_json.setdefault("paper_slot_evidence", {})
        if isinstance(evidence, dict):
            evidence.update(observed)

    def _archive_epoch_skip_reason(self, intent: PaperIntent, epoch: dict[str, Any] | None) -> str:
        if not epoch:
            return ""
        source_dt = _parse_iso(intent.source_generated_at)
        reset_dt = _parse_iso(epoch.get("reset_at"))
        if source_dt and reset_dt and source_dt <= reset_dt:
            return "source_trade_plan_before_archive_epoch"
        return ""

    def _stale_skip_reason(self, intent: PaperIntent) -> tuple[str, dict[str, Any]]:
        max_age = int(self.config.max_trade_plan_age_sec or 0)
        if max_age <= 0:
            return "", {}
        source_dt = _parse_iso(intent.source_generated_at)
        if source_dt is None:
            return "", {}
        age = max(0.0, (datetime.now(timezone.utc) - source_dt).total_seconds())
        if age > max_age:
            return "source_trade_plan_stale_for_paper", {"source_trade_plan_age_sec": round(age, 3), "max_trade_plan_age_sec": max_age}
        return "", {"source_trade_plan_age_sec": round(age, 3), "max_trade_plan_age_sec": max_age}

    def _reentry_cooldown_skip_reason(self, intent: PaperIntent) -> tuple[str, dict[str, Any]]:
        cooldown = int(self.config.reentry_cooldown_sec or 0)
        if cooldown <= 0:
            return "", {}
        last = self.store.last_closed_slot(intent.strategy_line, intent.symbol, intent.side)
        if not last:
            return "", {}
        closed_dt = _parse_iso(last.get("closed_at"))
        if closed_dt is None:
            return "", {}
        age = max(0.0, (datetime.now(timezone.utc) - closed_dt).total_seconds())
        if age >= cooldown:
            return "", {}
        exit_reason = str(last.get("exit_reason") or "").lower()
        enabled = (
            (exit_reason == "sl" and self.config.reentry_cooldown_after_sl)
            or (exit_reason == "tp" and self.config.reentry_cooldown_after_tp)
            or (exit_reason == str(self.config.archive_force_close_exit_reason).lower() and self.config.reentry_cooldown_after_forced_close)
        )
        if not enabled:
            return "", {}
        reason = "reentry_cooldown_after_sl" if exit_reason == "sl" else "reentry_cooldown_after_forced_close"
        return reason, {
            "cooldown_sec": cooldown,
            "remaining_cooldown_sec": round(cooldown - age, 3),
            "last_order": last,
        }

    def process_pending_entries(self) -> dict[str, Any]:
        entered = []
        waiting = []
        for order in self.store.open_orders():
            if order["status"] != "pending_entry":
                continue
            candle = self._latest_candle(order["symbol"])
            if candle is None and self._requires_historical_entry_candle():
                waiting.append(order["id"])
                continue
            if not self._entry_touched(order, candle):
                waiting.append(order["id"])
                continue
            reference = self._entry_reference(order, candle)
            planned_notional = float(order.get("planned_notional_usdt") or 0)
            notional = planned_notional if planned_notional > 0 else float(order["margin_usdt"]) * float(order["leverage"])
            cost = self._cost(reference, order["side"], "entry", notional, source=order, candle=candle)
            quantity = cost.notional_usdt / cost.fill_price if cost.fill_price else 0.0
            position_id = self.store.execute_entry(
                order,
                cost,
                quantity=quantity,
                at=self._execution_time_iso(candle),
                candle_ms=candle.open_time_ms if candle else None,
            )
            if position_id:
                entered.append({"order_id": order["id"], "position_id": position_id, "symbol": order["symbol"]})
        return {"entered": len(entered), "waiting": len(waiting), "items": entered}

    def process_open_positions(self) -> dict[str, Any]:
        closed = []
        updated = []
        for position in self.store.open_positions():
            candle = self._latest_candle(position["symbol"])
            if candle is None:
                continue
            trigger = self._exit_trigger(position, candle)
            if trigger is None:
                self.store.update_unrealized(position, candle.close)
                updated.append(position["id"])
                continue
            reason, reference = trigger
            qty = float(position["remaining_quantity"])
            cost = self._cost(reference, position["side"], reason, qty * reference, source=position, candle=candle)
            gross = paper_pnl(position["side"], float(position["entry_price"]), cost.fill_price, qty)
            did_close = self.store.close_position(
                position,
                cost,
                gross_pnl=gross,
                exit_reason=reason.upper(),
                at=self._execution_time_iso(candle),
                candle_ms=candle.open_time_ms,
            )
            if did_close:
                closed.append({"position_id": position["id"], "symbol": position["symbol"], "reason": reason})
        return {"closed": len(closed), "updated": len(updated), "items": closed}

    def _latest_candle(self, symbol: str) -> Candle | None:
        candles = self.candle_provider.get_1m(symbol, limit=1)
        return candles[-1] if candles else None

    def _requires_historical_entry_candle(self) -> bool:
        return bool(getattr(self.candle_provider, "historical_time_mode", False))

    def _execution_time_iso(self, candle: Candle | None) -> str:
        if candle is not None and getattr(self.candle_provider, "historical_time_mode", False):
            return datetime.fromtimestamp(int(candle.open_time_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return utc_now_iso()

    def _entry_touched(self, order: dict[str, Any], candle: Candle | None) -> bool:
        if order["order_type"] == "market":
            return True
        if candle is None:
            return False
        entry = float(order["entry_price"])
        if order["order_type"] == "limit":
            return candle.low <= entry <= candle.high
        if order["order_type"] == "trigger":
            if order["side"].upper() == "LONG":
                return candle.high >= entry
            return candle.low <= entry
        return False

    def _exit_trigger(self, position: dict[str, Any], candle: Candle) -> tuple[str, float] | None:
        side = position["side"].upper()
        sl = float(position["stop_loss"])
        tp = float(position["take_profit"])
        if side == "LONG":
            if self.config.trigger_sl_first and candle.low <= sl:
                return ("sl", sl)
            if candle.high >= tp:
                return ("tp", tp)
            if candle.low <= sl:
                return ("sl", sl)
        else:
            if self.config.trigger_sl_first and candle.high >= sl:
                return ("sl", sl)
            if candle.low <= tp:
                return ("tp", tp)
            if candle.high >= sl:
                return ("sl", sl)
        return None

    def _entry_reference(self, order: dict[str, Any], candle: Candle | None) -> float:
        planned = float(order["entry_price"])
        if str(self.config.fill_model_mode or "").lower() != "realistic_1m" or candle is None:
            return planned
        candle_ref = float(candle.open or candle.close or planned)
        if order["side"].upper() == "LONG":
            reference = max(planned, candle_ref)
        else:
            reference = min(planned, candle_ref)
        return self._cap_entry_drift(planned, reference, order["side"])

    def _cap_entry_drift(self, planned: float, reference: float, side: str) -> float:
        if planned <= 0:
            return reference
        max_bps = max(0.0, float(self.config.max_entry_drift_bps or 0))
        drift_bps = abs(reference - planned) / planned * 10_000
        if max_bps <= 0 or drift_bps <= max_bps:
            return reference
        cap = planned * max_bps / 10_000
        return planned + cap if side.upper() == "LONG" else planned - cap

    def _cost(
        self,
        reference: float,
        side: str,
        action: str,
        notional: float,
        *,
        source: dict[str, Any] | None = None,
        candle: Candle | None = None,
    ) -> Any:
        fee_bps = self.config.taker_fee_bps
        source = source or {}
        planned_entry = _float(source.get("entry_price") or source.get("planned_entry_price"))
        slippage_bps, source_name, liquidity_penalty, volatility_penalty = self._slippage_bps(source, candle, action)
        fill_price = adverse_fill_price(reference, side, action, slippage_bps)
        quantity = notional / fill_price if fill_price else 0.0
        entry_drift_bps = 0.0
        if planned_entry and action == "entry":
            entry_drift_bps = round(abs(reference - planned_entry) / planned_entry * 10_000, 4)
        return build_fill_cost(
            reference_price=reference,
            fill_price=fill_price,
            side=side,
            action=action,
            quantity=quantity,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            cost_source="paper_realistic_1m" if str(self.config.fill_model_mode).lower() == "realistic_1m" else "paper_default",
            planned_entry_price=planned_entry,
            entry_drift_bps=entry_drift_bps,
            fill_delay_sec=self._fill_delay_sec(source),
            fill_model=str(self.config.fill_model_mode or "fixed_1m"),
            slippage_source=source_name,
            liquidity_penalty_bps=liquidity_penalty,
            volatility_penalty_bps=volatility_penalty,
            same_candle_policy=str(self.config.same_candle_sl_tp_policy or "sl_first"),
            source_generated_at=source.get("source_generated_at"),
            consumed_at=source.get("consumed_at"),
        )

    def _slippage_bps(self, source: dict[str, Any], candle: Candle | None, action: str) -> tuple[float, str, float, float]:
        default = (
            float(self.config.default_market_slippage_bps)
            if self.config.default_market_slippage_bps is not None
            else float(self.config.default_slippage_bps)
        )
        if str(self.config.fill_model_mode or "").lower() != "realistic_1m":
            return float(self.config.default_slippage_bps), "default", 0.0, 0.0
        plan_source = 0.0
        if self.config.use_trade_plan_slippage:
            guards = source.get("guards") if isinstance(source.get("guards"), dict) else {}
            if not guards and isinstance(source.get("plan_guards"), dict):
                guards = source.get("plan_guards") or {}
            plan_source = _first_float(
                guards,
                source,
                "estimated_slippage_bps",
                "slippage_bps",
                "expected_slippage_bps",
                "market_slippage_bps",
            )
        base = plan_source if plan_source > 0 else default
        liquidity_penalty = self._liquidity_penalty_bps(source)
        volatility_penalty = self._volatility_penalty_bps(candle)
        got = base + liquidity_penalty + volatility_penalty
        cap = max(0.0, float(self.config.max_allowed_paper_slippage_bps or 0))
        if cap and got > cap:
            got = cap
        source_name = "trade_plan_dynamic" if plan_source > 0 else "default_dynamic"
        if action != "entry":
            source_name = f"{source_name}_exit"
        return got, source_name, liquidity_penalty, volatility_penalty

    def _liquidity_penalty_bps(self, source: dict[str, Any]) -> float:
        if not self.config.use_liquidity_profile:
            return 0.0
        guards = source.get("guards") if isinstance(source.get("guards"), dict) else {}
        if not guards and isinstance(source.get("plan_guards"), dict):
            guards = source.get("plan_guards") or {}
        tier = str(guards.get("symbol_execution_tier") or guards.get("liquidity_tier") or "").lower()
        if tier in {"thin", "weak", "tier3", "low"}:
            return max(0.0, float(self.config.default_slippage_bps) * (float(self.config.thin_book_slippage_mult) - 1.0))
        spread = _first_float(guards, source, "spread_bps", "book_spread_bps")
        if spread > 0:
            return max(0.0, spread * 0.25)
        return 0.0

    def _volatility_penalty_bps(self, candle: Candle | None) -> float:
        if candle is None or candle.close <= 0:
            return 0.0
        candle_range_bps = abs(float(candle.high) - float(candle.low)) / float(candle.close) * 10_000
        return max(0.0, candle_range_bps * float(self.config.volatility_slippage_mult or 0))

    def _fill_delay_sec(self, source: dict[str, Any]) -> float | None:
        consumed = _parse_iso(source.get("consumed_at"))
        generated = _parse_iso(source.get("source_generated_at"))
        if consumed and generated:
            return round(max(0.0, (consumed - generated).total_seconds()), 3)
        if int(self.config.entry_delay_sec or 0) > 0:
            return float(self.config.entry_delay_sec)
        return None


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        got = datetime.fromisoformat(text)
        if got.tzinfo is None:
            got = got.replace(tzinfo=timezone.utc)
        return got.astimezone(timezone.utc)
    except ValueError:
        return None


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_float(row_a: dict[str, Any], row_b: dict[str, Any] | None = None, *keys: str) -> float:
    for row in (row_a, row_b or {}):
        for key in keys:
            got = _float(row.get(key))
            if got is not None:
                return got
    return 0.0
