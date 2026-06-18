from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_config_gate_lineage_contract.json"


def canonical_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def main() -> int:
    data = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert data["contract_version"] == "step29_config_gate_lineage_v1"
    boundary = data["hard_boundary"]
    assert boundary["change_runtime_config"] is False
    assert boundary["change_trade_gate_behavior"] is False
    assert boundary["source_db_write_back"] is False
    assert boundary["sidecar_can_be_runtime_config_source"] is False
    required = set(data["lineage_payload"]["required_keys"])
    for key in (
        "config_hash",
        "gate_hash",
        "gate_rule_json",
        "gate_features_json",
        "fill_model",
        "cost_source",
        "missing_fields_json",
    ):
        assert key in required, key
    payload = {"b": 2, "a": {"x": 1}}
    assert canonical_hash(payload) == canonical_hash({"a": {"x": 1}, "b": 2})
    print(
        json.dumps(
            {
                "contract": data["contract_version"],
                "required_keys": len(required),
                "comparison_keys": len(data["comparison_keys"]),
                "status": "ok",
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
