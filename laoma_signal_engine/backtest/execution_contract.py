"""Execution-contract governance for promotion-facing backtests."""

from __future__ import annotations

from typing import Any


DEFAULT_BACKTEST_EXECUTION_CONTRACT = "paper_equivalent"
PAPER_ENGINE_EXECUTION_CONTRACT = "paper_engine"
PAPER_EQUIVALENT_EXECUTION_CONTRACT = "paper_equivalent"
LEGACY_BACKTEST_EXECUTION_CONTRACT = "legacy_backtest_only"
RERUN_REQUIRED_BLOCK_REASON = "rerun_required_under_paper_equivalent"

FIELD_COMPARABILITY = {
    "order_intent": "paper.adapter.intent_from_plan_document -> PaperIntent",
    "risk_trade_gate": "paper.v5_gate.evaluate_paper_v5_trade_gate",
    "order_state_machine": "PaperEngine.consume_trade_plans/process_pending_entries/process_open_positions",
    "ledger": "paper_intent_inbox/paper_skip_ledger/paper_orders/paper_fills/paper_positions",
    "fill_position_model": "PaperEngine historical candle provider with PaperStore fill/position state",
}


def paper_equivalent_metadata(
    *,
    execution_contract_version: str,
    paper_adapter_version: str,
    paper_gate_version: str,
    paper_fill_model: str | None = None,
) -> dict[str, Any]:
    """Metadata for results that may be compared with paper and promoted."""

    return {
        "default_backtest_execution_contract": DEFAULT_BACKTEST_EXECUTION_CONTRACT,
        "execution_contract": PAPER_EQUIVALENT_EXECUTION_CONTRACT,
        "execution_contract_version": execution_contract_version,
        "promotion_allowed": True,
        "promotion_block_reason": "",
        "paper_adapter_version": paper_adapter_version,
        "paper_gate_version": paper_gate_version,
        "paper_fill_model": paper_fill_model,
        "field_comparability": dict(FIELD_COMPARABILITY),
        "equivalence_claim": "field_mapped_equivalent_to_paper_execution_chain",
    }


def legacy_backtest_metadata(*, reason: str, engine_mode: str | None = None) -> dict[str, Any]:
    """Metadata for research-only backtests that bypass the paper chain."""

    return {
        "default_backtest_execution_contract": DEFAULT_BACKTEST_EXECUTION_CONTRACT,
        "execution_contract": LEGACY_BACKTEST_EXECUTION_CONTRACT,
        "execution_contract_version": "legacy.direct_fill.v1",
        "promotion_allowed": False,
        "promotion_block_reason": RERUN_REQUIRED_BLOCK_REASON,
        "legacy_backtest_reason": reason,
        "legacy_engine_mode": engine_mode,
        "required_execution_contract_for_promotion": DEFAULT_BACKTEST_EXECUTION_CONTRACT,
        "field_comparability": {
            "order_intent": "not_comparable_without_paper_adapter",
            "risk_trade_gate": "not_comparable_without_paper_gate_or_field_mapping",
            "order_state_machine": "not_comparable_without_paper_engine",
            "ledger": "not_comparable_without_paper_ledger",
            "fill_position_model": "not_comparable_without_paper_fill_position_state",
        },
    }


def is_promotion_allowed(payload: dict[str, Any]) -> bool:
    return str(payload.get("execution_contract") or "") in {
        PAPER_ENGINE_EXECUTION_CONTRACT,
        PAPER_EQUIVALENT_EXECUTION_CONTRACT,
    } and bool(payload.get("promotion_allowed"))
