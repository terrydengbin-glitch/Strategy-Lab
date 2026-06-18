from __future__ import annotations

from fastapi.testclient import TestClient

from laoma_signal_engine.api.app import app


def test_config_field_impact_summary_contract() -> None:
    client = TestClient(app)
    response = client.get("/api/config/field-impact-summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    data = payload["data"]
    assert data["field_count"] > 0
    assert data["unknown_field_count"] == 0
    assert data["direct_executable_field_count"] > 0


def test_config_effective_strategy6_contract_marks_base_plan_inheritance() -> None:
    client = TestClient(app)
    response = client.get("/api/config/effective?strategy_line=strategy6")
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["strategy_line"] == "strategy6"
    assert data["inherits_from"] == "without_micro"
    assert data["counts"]["direct_executable"] > 0
    assert data["live_executable_source"]["base_plan"] == "without_micro"
    assert any("without_micro" in note for note in data["notes"])


def test_config_legacy_fields_are_not_current_direct_executable() -> None:
    client = TestClient(app)
    response = client.get("/api/config/legacy-fields")
    assert response.status_code == 200
    fields = response.json()["data"]["fields"]
    legacy_gate = [
        row for row in fields if row["field_path"] == "trade_plan_lines.without_micro.trade_quality_gate.enabled"
    ]
    assert legacy_gate
    assert legacy_gate[0]["status"] == "disabled"
    assert legacy_gate[0]["direct_executable_impact"] is False


def test_config_ui_schema_groups_have_primary_and_legacy() -> None:
    client = TestClient(app)
    response = client.get("/api/config/ui-schema")
    assert response.status_code == 200
    groups = response.json()["data"]["groups"]
    assert groups["primary"]["count"] > 0
    assert groups["hide_legacy"]["count"] > 0
