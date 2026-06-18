from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from laoma_signal_engine.core.json_io import write_json_atomic
from laoma_signal_engine.decision.trade_plan_line_models import TradePlanItem, TradePlanLineDocument
from laoma_signal_engine.paper.adapter import adapt_documents, load_trade_plan_documents
from laoma_signal_engine.strategy5.evidence import paths, write_strategy5_outputs


def _write_fixture_docs(root: Path) -> None:
    stale_factor = {
        "schema_version": "1.6",
        "generated_at": "2026-06-04T00:00:00Z",
        "source": "stale_factor_snapshot",
        "items": [{"symbol": "BTCUSDT", "move_side": "up"}],
    }
    factor = {
        "schema_version": "1.6",
        "generated_at": "2026-06-05T00:00:00Z",
        "source": "factor_snapshot_without_ofi_cvd",
        "items": [
            {
                "symbol": "BTCUSDT",
                "move_side": "up",
                "primary_15m": {
                    "price_ret": 1.2,
                    "volume_ratio": 2.1,
                    "taker_buy_ratio": 0.62,
                    "kline_cvd_state": "buy_dominant",
                    "range_pos": 0.62,
                },
                "trigger_5m": {"price_ret": 0.4},
                "entry_1m": {"atr": 10.0},
            }
        ],
    }
    light = {
        "schema_version": "test",
        "generated_at": "2026-06-05T00:00:00Z",
        "source": "fixture_futures_light_snapshot",
        "items": [{"symbol": "BTCUSDT"}],
    }
    plan = TradePlanLineDocument(
        generated_at="2026-06-05T00:00:05Z",
        run_id="BASE_RUN",
        cycle_id="cycle_BASE_RUN",
        source="trade_plan_without_micro",
        micro_mode="none",
        status="ok",
        count=1,
        executable_count=1,
        plans=[
            TradePlanItem(
                symbol="BTCUSDT",
                decision="LONG",
                action="ENTER_MARKET",
                entry_mode="MARKET",
                estimated_entry_price=100.0,
                stop_loss=99.0,
                take_profit=101.0,
                risk_per_unit=1.0,
                reward_per_unit=1.0,
                rr=1.0,
                executable=True,
                confidence=80,
                reason_codes=["fixture"],
                guards={"line": "without_micro"},
                position_sizing={"quantity": 1.0, "notional_usdt": 100.0, "margin_usdt": 10.0},
            )
        ],
    ).model_dump(mode="json")
    write_json_atomic(root / "DATA" / "factors" / "latest_factor_snapshot.json", stale_factor)
    write_json_atomic(root / "DATA" / "factors" / "latest_factor_snapshot_withoutoficvd.json", factor)
    write_json_atomic(root / "DATA" / "market" / "futures_light_snapshot.json", light)
    write_json_atomic(root / "DATA" / "decisions" / "latest_trade_plan_without_micro.json", plan)


def test_strategy5_generates_trade_plan_and_ledger(tmp_path: Path) -> None:
    _write_fixture_docs(tmp_path)

    result = write_strategy5_outputs(tmp_path, run_id="RUN5", cycle_id="cycle_RUN5")

    assert result["status"] == "ok"
    assert result["count"] == 1
    assert Path(result["output_path"]).is_file()
    assert Path(result["archive_path"]).is_file()
    doc = paths(tmp_path).latest_trade_plan.read_text(encoding="utf-8")
    assert "trade_plan_strategy5" in doc
    assert "strategy5_evidence_id" in doc
    parsed_doc = json.loads(doc)
    assert parsed_doc["input_refs"]["factor_generated_at"] == "2026-06-05T00:00:00Z"
    assert parsed_doc["input_refs"]["factor_snapshot_selected_reason"] == "latest_with_required_evidence"
    assert parsed_doc["input_refs"]["factor_path"].endswith("latest_factor_snapshot_withoutoficvd.json")
    archive_doc = Path(result["archive_path"]).read_text(encoding="utf-8")
    assert "strategy5_trade_plan_archive_path" in archive_doc

    with sqlite3.connect(paths(tmp_path).db) as con:
        rows = con.execute("select symbol, trigger_side, legacy_side from strategy5_evidence").fetchall()
    assert rows == [("BTCUSDT", "LONG", "LONG")]

    docs = load_trade_plan_documents(tmp_path)
    adapted = adapt_documents({"strategy5": docs["strategy5"]})
    assert len(adapted["intents"]) == 1
    intent = adapted["intents"][0]
    assert intent.strategy_line == "strategy5"
    assert intent.signal_class == "strategy5_direction_evidence"
    assert intent.source_run_id == "RUN5"


def test_strategy5_base_blocked_keeps_evidence_usable_reason(tmp_path: Path) -> None:
    _write_fixture_docs(tmp_path)
    plan_path = tmp_path / "DATA" / "decisions" / "latest_trade_plan_without_micro.json"
    plan_doc = json.loads(plan_path.read_text(encoding="utf-8"))
    plan_doc["plans"][0]["executable"] = False
    plan_doc["plans"][0]["action"] = "WAIT"
    plan_doc["plans"][0]["entry_mode"] = "WAIT_CONFIRMATION"
    plan_doc["plans"][0]["reason_codes"] = ["base_range_room_blocked"]
    plan_doc["executable_count"] = 0
    write_json_atomic(plan_path, plan_doc)

    result = write_strategy5_outputs(tmp_path, run_id="RUN5B", cycle_id="cycle_RUN5B")

    assert result["status"] == "ok"
    doc = json.loads(paths(tmp_path).latest_trade_plan.read_text(encoding="utf-8"))
    plan = doc["plans"][0]
    assert plan["executable"] is False
    assert "base_range_room_blocked" in plan["reason_codes"]
    assert "strategy5_base_trade_plan_not_executable" in plan["reason_codes"]
    assert "strategy5_required_evidence_missing" not in plan["reason_codes"]
    evidence = json.loads(paths(tmp_path).latest_evidence.read_text(encoding="utf-8"))["items"][0]
    assert evidence["evidence_quality"]["usable"] is True
