"""STEP6.0 LLM factor assist (mocked DeepSeek; no network)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.llm.run_factor_assist import (
    default_factor_assist_pairs,
    run_llm_factor_assist_one,
    run_llm_factor_assist_one_safe,
    run_llm_factor_assist_twice_safe,
)


def _minimal_factor(path: Path) -> None:
    doc = {
        "schema_version": "1.6",
        "generated_at": "2026-01-01T00:00:00Z",
        "source": "factor_snapshot",
        "count": 1,
        "items": [{"symbol": "BTCUSDT"}],
    }
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8", newline="")


def _assistant_payload_ok() -> str:
    row = {
        "symbol": "BTCUSDT",
        "llm_bias": "NEUTRAL",
        "action_hint": "wait",
        "confidence_0_100": 40,
        "key_reasons": ["test"],
        "warnings": [],
    }
    return json.dumps({"decisions": [row]}, ensure_ascii=False)


def test_default_pairs_under_project_root(tmp_path: Path) -> None:
    pr = tmp_path / "repo"
    pr.mkdir()
    pairs = default_factor_assist_pairs(pr)
    assert len(pairs) == 2
    assert pairs[0][0].name == "latest_factor_snapshot.json"
    assert pairs[1][0].name == "latest_factor_snapshot_withoutoficvd.json"


def test_run_one_mock_chat_writes_ok(tmp_path: Path) -> None:
    pr = tmp_path
    fac = pr / "factor.json"
    prompt = pr / "p.txt"
    out = pr / "out.json"
    _minimal_factor(fac)
    prompt.write_text("Hello\n{{FACTOR_JSON}}\n", encoding="utf-8", newline="")

    def _chat(content: str) -> tuple[str, dict[str, str]]:
        assert "BTCUSDT" in content
        return _assistant_payload_ok(), {"prompt_tokens": 1}

    run_llm_factor_assist_one(
        project_root=pr,
        factor_path=fac,
        prompt_path=prompt,
        output_path=out,
        chat_fn=_chat,
    )
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["status"] == "ok"
    assert loaded["count"] == 1
    assert loaded["decisions"][0]["symbol"] == "BTCUSDT"


def test_one_safe_missing_api_key_writes_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    pr = tmp_path
    fac = pr / "factor.json"
    prompt = pr / "p.txt"
    out = pr / "out.json"
    _minimal_factor(fac)
    prompt.write_text("x {{FACTOR_JSON}}", encoding="utf-8", newline="")

    rc = run_llm_factor_assist_one_safe(
        project_root=pr,
        factor_path=fac,
        prompt_path=prompt,
        output_path=out,
        chat_fn=None,
    )
    assert rc == EXIT_CONFIG
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["status"] == "error"
    assert "DEEPSEEK_API_KEY" in loaded["error_message"]


def test_twice_safe_merges_worst_exit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    pr = tmp_path
    fac_a = pr / "a.json"
    fac_b = pr / "b.json"
    p_a = pr / "a.txt"
    p_b = pr / "b.txt"
    o_a = pr / "oa.json"
    o_b = pr / "ob.json"
    _minimal_factor(fac_a)
    _minimal_factor(fac_b)
    p_a.write_text("{{FACTOR_JSON}}", encoding="utf-8", newline="")
    p_b.write_text("{{FACTOR_JSON}}", encoding="utf-8", newline="")

    calls: list[str] = []

    def _chat_good(content: str) -> tuple[str, dict[str, str]]:
        calls.append("good")
        return _assistant_payload_ok(), {}

    def _chat_force_config(_content: str) -> tuple[str, dict[str, str]]:
        raise OSError("DEEPSEEK_API_KEY is not set")

    rc = run_llm_factor_assist_twice_safe(
        project_root=pr,
        stdout_json=False,
        pairs=[
            (fac_a, p_a, o_a),
            (fac_b, p_b, o_b),
        ],
        chat_fn=_chat_good,
    )
    assert rc == EXIT_SUCCESS
    assert len(calls) == 2

    rc2 = run_llm_factor_assist_twice_safe(
        project_root=pr,
        pairs=[
            (fac_a, p_a, o_a),
            (fac_b, p_b, o_b),
        ],
        chat_fn=_chat_force_config,
    )
    assert rc2 == EXIT_CONFIG


def test_schema_rejects_non_16(tmp_path: Path) -> None:
    pr = tmp_path
    fac = pr / "factor.json"
    fac.write_text(
        json.dumps({"schema_version": "1.0", "count": 0, "items": []}, ensure_ascii=False),
        encoding="utf-8",
        newline="",
    )
    prompt = pr / "p.txt"
    prompt.write_text("{{FACTOR_JSON}}", encoding="utf-8", newline="")
    out = pr / "out.json"

    rc = run_llm_factor_assist_one_safe(
        project_root=pr,
        factor_path=fac,
        prompt_path=prompt,
        output_path=out,
        chat_fn=lambda _c: (_assistant_payload_ok(), {}),
    )
    assert rc == EXIT_INTERNAL
    body = json.loads(out.read_text(encoding="utf-8"))
    assert body["status"] == "error"
