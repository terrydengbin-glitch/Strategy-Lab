from laoma_signal_engine.training_readiness.handoff_audit import build_handoff_summary


def test_step2921_blocked_handoff_preserves_blockers_and_paths() -> None:
    status = {
        "dataset_status": "blocked",
        "allowed_for_training": False,
        "allowed_for_llm_training": False,
        "sidecar_db": "DATA/research/trade_snapshots/trade_snapshots.db",
        "sample_count": 2,
        "blocking_reasons": ["cost_fields_coverage"],
        "cost_fields_coverage": 0.0,
        "source_mode_counts": {"paper": 2},
    }
    manifest = {
        "manifest_id": "m1",
        "status_path": "DATA/research/trade_snapshots/status.json",
        "dataset_hash": "abc",
        "split_manifest_hash": "def",
    }

    result = build_handoff_summary(status, manifest)

    assert result["handoff_status"] == "BLOCKED"
    assert result["allowed_for_training"] is False
    assert result["read_only_contract"]["ai_trader_may_write_source_db"] is False
    assert "cost_fields_coverage" in result["blocking_task_hints"]


def test_step2921_pass_handoff_when_manifest_allowed() -> None:
    result = build_handoff_summary({"allowed_for_training": True, "dataset_status": "training_ready"}, {})

    assert result["handoff_status"] == "PASS"
    assert result["blocking_reasons"] == []
