from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_trade_snapshot_schema_contract.json"


def main() -> int:
    data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert data["schema_version"] == "step29_trade_snapshot_v1"
    for table_name in (
        "trade_snapshot_events",
        "trade_training_samples",
        "trade_snapshot_source_refs",
        "trade_snapshot_manifests",
        "trade_snapshot_coverage_audits",
    ):
        table = data["tables"][table_name]
        assert table["required_columns"], table_name
    samples = data["tables"]["trade_training_samples"]["required_columns"]
    assert "decision_time_input_json" in samples
    assert "post_trade_outcome_json" in samples
    assert "label_json" in samples
    post_trade = set(data["post_trade_only_fields"])
    forbidden_roles = [
        role
        for role, spec in data["field_roles"].items()
        if "decision_time_input_json" in spec.get("forbidden_in", [])
    ]
    assert {"outcome", "label"}.issubset(set(forbidden_roles))
    for field in post_trade:
        assert field, "empty post-trade field"
    ddl = "\n".join(data["ddl"])
    for table_name in data["tables"]:
        assert table_name in ddl, table_name
    print(
        json.dumps(
            {
                "schema": data["schema_version"],
                "tables": len(data["tables"]),
                "post_trade_only_fields": len(post_trade),
                "status": "ok",
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
