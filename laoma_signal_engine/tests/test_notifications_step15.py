from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

import laoma_signal_engine.api.services as api_services
from laoma_signal_engine.api.app import app
from laoma_signal_engine.cli import main
from laoma_signal_engine.notifications.card import build_trade_plan_card, card_summary
from laoma_signal_engine.notifications.config import FeishuConfig
from laoma_signal_engine.notifications.delivery import read_delivery_history
from laoma_signal_engine.notifications.selector import mock_trade_plan_docs, select_trade_plan_signals
from laoma_signal_engine.notifications.service import send_trade_plan_notifications


def _paper_summary() -> dict:
    return {
        "stats": {
            "by_line": {
                "without_micro": {"total_orders": 12, "win_rate": 58.3333},
                "micro_fast": {"total_orders": 8, "win_rate": 62.5},
                "micro_full": {"total_orders": 4, "win_rate": 50.0},
            }
        }
    }


def test_step15_selector_executable_only_and_three_strategy_names() -> None:
    cfg = FeishuConfig()

    got = select_trade_plan_signals(mock_trade_plan_docs(), config=cfg, paper_summary=_paper_summary())

    assert got["selected_counts"] == {"without_micro": 1, "micro_fast": 1, "micro_full": 1}
    names = {item["strategy_name"] for item in got["selected"]}
    assert names == {"异动壹号", "异动贰号", "异动叁号"}
    assert {item["side_text"] for item in got["selected"]} == {"多", "空"}


def test_step1513_selector_can_filter_single_strategy_line() -> None:
    cfg = FeishuConfig()

    got = select_trade_plan_signals(
        mock_trade_plan_docs(),
        config=cfg,
        paper_summary=_paper_summary(),
        line="micro_fast",
    )

    assert got["selected_counts"] == {"without_micro": 0, "micro_fast": 1, "micro_full": 0}
    assert [item["strategy_line"] for item in got["selected"]] == ["micro_fast"]


def test_step15_card_contains_minimal_trade_fields_and_stats() -> None:
    signal = select_trade_plan_signals(
        mock_trade_plan_docs(),
        config=FeishuConfig(),
        paper_summary=_paper_summary(),
    )["selected"][0]

    card = build_trade_plan_card(signal)
    summary = card_summary(card)

    assert card["header"]["title"]["content"] == "【异动壹号】有效信号"
    assert "交易对" in summary["text"]
    assert "进场价格" in summary["text"]
    assert "SL" in summary["text"]
    assert "TP" in summary["text"]
    assert "总订单：12" in summary["text"]
    assert "胜率：58.33%" in summary["text"]


def test_step15_mock_send_writes_delivery_history(tmp_path: Path) -> None:
    payload = send_trade_plan_notifications(tmp_path, mock_signals=True, mock_send=True, config=FeishuConfig())

    assert payload["selected"] == {"without_micro": 1, "micro_fast": 1, "micro_full": 1}
    assert payload["status"] == "sent"
    assert len(payload["deliveries"]) == 3
    assert {row["status"] for row in payload["deliveries"]} == {"mock_sent"}
    history = read_delivery_history(tmp_path)
    assert len(history["deliveries"]) == 3
    assert all(row["strategy_name"].startswith("异动") for row in history["deliveries"])
    assert (tmp_path / "DATA/notifications/latest_delivery_report.json").exists()


def test_step1514_partial_delivery_records_response_and_nonfatal_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = {"count": 0}

    def fake_send(*_: object, **__: object) -> dict:
        calls["count"] += 1
        if calls["count"] == 1:
            return {"ok": True, "sent": True, "status_code": 200, "response": {"code": 0}, "error": None}
        return {
            "ok": False,
            "sent": True,
            "status_code": 200,
            "response": {"code": 19021, "msg": "bad card"},
            "error": "feishu webhook returned failure",
        }

    monkeypatch.setattr("laoma_signal_engine.notifications.service.send_interactive_card", fake_send)

    payload = send_trade_plan_notifications(
        tmp_path,
        mock_signals=True,
        force_enabled=True,
        config=FeishuConfig(enabled=True, webhook_url="https://example.invalid/hook"),
    )

    assert payload["status"] == "partial"
    assert payload["delivery_counts"]["success"] == 1
    assert payload["delivery_counts"]["failed"] == 2
    failed = [row for row in payload["deliveries"] if row["status"] == "failed"]
    assert failed[0]["status_code"] == 200
    assert failed[0]["response"]["code"] == 19021


def test_step1515_disabled_runtime_report_appends_per_line_delivery_history(tmp_path: Path) -> None:
    payload = send_trade_plan_notifications(
        tmp_path,
        mock_signals=True,
        config=FeishuConfig(enabled=False, webhook_url="https://example.invalid/hook"),
    )

    assert payload["status"] == "skipped"
    assert payload["skip_reason"] == "feishu_disabled"
    assert len(payload["deliveries"]) == 3
    assert {row["status"] for row in payload["deliveries"]} == {"skipped"}
    assert {row["skip_reason"] for row in payload["deliveries"]} == {"feishu_disabled"}
    history = read_delivery_history(tmp_path)["deliveries"]
    assert len(history) == 3
    assert {row["strategy_line"] for row in history} == {"without_micro", "micro_fast", "micro_full"}
    assert (tmp_path / "DATA/notifications/latest_delivery_report.json").exists()


def test_step15_api_send_trade_plans_mock(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(api_services, "PROJECT_ROOT", tmp_path)
    client = TestClient(app)

    response = client.post(
        "/api/notifications/feishu/send-trade-plans",
        json={"mock_signals": True, "mock_send": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert payload["data"]["selected"]["micro_fast"] == 1
    deliveries = client.get("/api/notifications/deliveries").json()
    assert len(deliveries["data"]["deliveries"]) == 3


def test_step15_cli_mock_send(tmp_path: Path, capsys) -> None:
    code = main(
        [
            "feishu-send-trade-plans",
            "--project-root",
            str(tmp_path),
            "--mock-signals",
            "--mock-send",
            "--stdout-json",
        ]
    )

    assert code == 0
    out = capsys.readouterr().out
    assert '"micro_full": 1' in out
