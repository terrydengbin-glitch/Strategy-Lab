from __future__ import annotations

from laoma_signal_engine.api.contracts import PipelineRunRequest


def test_pipeline_run_request_accepts_strategy5_and_strategy6_lines() -> None:
    req = PipelineRunRequest(lines=["without_micro", "micro_fast", "strategy5", "strategy6"])

    assert req.lines == ["without_micro", "micro_fast", "strategy5", "strategy6"]


def test_pipeline_run_request_all_includes_strategy5_and_strategy6() -> None:
    req = PipelineRunRequest(lines=["all"])

    assert req.lines == ["without_micro", "micro_fast", "micro_full", "strategy4", "strategy5", "strategy6"]
