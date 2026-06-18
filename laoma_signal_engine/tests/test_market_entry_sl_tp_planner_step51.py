"""STEP5.1 market-entry SL/TP planner tests."""

from __future__ import annotations

import json
from pathlib import Path

from laoma_signal_engine.decision.market_entry_direction_gate import build_market_entry_direction_document
from laoma_signal_engine.decision.market_entry_models import MarketEntryDirectionDocument
from laoma_signal_engine.decision.market_entry_sl_tp_planner import (
    build_market_entry_plan_document,
    run_apply_market_entry_sl_tp_planner_safe,
)
from laoma_signal_engine.tests.test_market_entry_direction_gate_step43 import (
    GEN,
    _factor_with_market_entry,
    _micro_doc,
    _refresh,
)


def _direction_and_refresh():
    factor = _factor_with_market_entry()
    refresh = _refresh(factor)
    direction = build_market_entry_direction_document(
        factor=factor,
        refresh=refresh,
        micro=_micro_doc(fast_ready=True, full_ready=False, z=1.0),
        generated_at=GEN,
    )
    return direction, refresh


def test_long_market_plan_uses_market_entry_and_positive_rr() -> None:
    direction, refresh = _direction_and_refresh()
    doc = build_market_entry_plan_document(
        direction_doc=direction,
        refresh_doc=refresh,
        generated_at=GEN,
    )
    p = doc.plans[0]
    assert p.executable is True
    assert p.entry_mode == "MARKET"
    assert p.estimated_entry_price == 100.0
    assert p.stop_loss is not None and p.stop_loss < 100.0
    assert p.take_profit is not None and p.take_profit > 100.0
    assert p.rr is not None and p.rr >= 1.0


def test_no_trade_direction_has_no_prices() -> None:
    direction, refresh = _direction_and_refresh()
    raw = direction.model_dump(mode="json")
    raw["decisions"][0]["decision"] = "NO_MARKET_ENTRY"
    raw["decisions"][0]["direction"] = "NONE"
    raw["decisions"][0]["action"] = "NO_TRADE"
    direction2 = MarketEntryDirectionDocument.model_validate(raw)
    doc = build_market_entry_plan_document(
        direction_doc=direction2,
        refresh_doc=refresh,
        generated_at=GEN,
    )
    p = doc.plans[0]
    assert p.executable is False
    assert p.entry_mode == "NONE"
    assert p.estimated_entry_price is None
    assert "direction_gate_no_entry" in p.reason_codes


def test_planner_cli_writes_json(tmp_path: Path) -> None:
    direction, refresh = _direction_and_refresh()
    direction_p = tmp_path / "direction.json"
    refresh_p = tmp_path / "refresh.json"
    out_p = tmp_path / "plans.json"
    direction_p.write_text(json.dumps(direction.model_dump(mode="json")), encoding="utf-8")
    refresh_p.write_text(json.dumps(refresh.model_dump(mode="json")), encoding="utf-8")
    code = run_apply_market_entry_sl_tp_planner_safe(
        project_root=tmp_path,
        direction_path=direction_p,
        refresh_path=refresh_p,
        output_path=out_p,
    )
    assert code == 0
    assert out_p.is_file()
