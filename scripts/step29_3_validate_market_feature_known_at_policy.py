from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_market_feature_known_at_policy.json"


def main() -> int:
    data = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    assert data["policy_version"] == "step29_market_feature_known_at_v1"
    assert data["default_scope"] == "pure_kline"
    alignment = data["event_alignment"]
    assert "feature_available_time_ms <= decision_time_ms" in alignment["known_at_ms_rule"]
    pure = data["feature_groups"]["pure_kline_default"]
    assert pure["default_in_decision_input"] is True
    for name, spec in pure["features"].items():
        assert spec["role"] == "input_feature", name
        assert "known_at" in spec, name
        assert spec["missing_policy"] == "nullable_and_report", name
    extended = data["feature_groups"]["extended_market_context"]
    assert extended["default_in_decision_input"] is False
    assert data["feature_groups"]["excluded_by_default"]["default_in_decision_input"] is False
    required = set(data["manifest_required_fields"])
    for field in (
        "feature_schema_version",
        "feature_schema_hash",
        "feature_available_time_ms",
        "known_at_pass",
        "missing_fields_json",
        "blocked_fields_json",
    ):
        assert field in required, field
    print(
        json.dumps(
            {
                "policy": data["policy_version"],
                "pure_kline_features": len(pure["features"]),
                "extended_features": len(extended["features"]),
                "status": "ok",
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
