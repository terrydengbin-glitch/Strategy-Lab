"""Run LLM assist: two factor JSON files plus two prompt .txt -> DATA/llm/out/. docs/STEP6.0."""

from __future__ import annotations

import logging
import os
import json
import sys
from pathlib import Path
from typing import Any, Protocol

from laoma_signal_engine.core.exit_codes import EXIT_CONFIG, EXIT_INTERNAL, EXIT_SUCCESS
from laoma_signal_engine.core.json_io import read_json_object
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.llm.deepseek_client import (
    DEFAULT_BASE_URL,
    DEFAULT_MODEL,
    parse_decisions_payload,
    post_chat_completions,
)
from laoma_signal_engine.llm.models import LlmAssistDecisionItem, LlmAssistDocument
from laoma_signal_engine.llm.prompt_loader import load_prompt_template, render_prompt
from laoma_signal_engine.llm.writer import atomic_write_llm_assist

log = logging.getLogger(__name__)


class ChatFn(Protocol):
    def __call__(self, user_content: str) -> tuple[str, dict[str, Any]]: ...


def _rel_or_abs(project_root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _validate_factor_shapes(factor_obj: dict[str, Any]) -> None:
    if factor_obj.get("schema_version") != "1.6":
        msg = "factor snapshot schema_version must be 1.6"
        raise ValueError(msg)
    count = factor_obj.get("count")
    items = factor_obj.get("items")
    if not isinstance(count, int) or not isinstance(items, list):
        msg = "factor snapshot missing count or items"
        raise ValueError(msg)
    if count != len(items):
        msg = f"factor count mismatch: {count} vs len(items)={len(items)}"
        raise ValueError(msg)


def default_factor_assist_pairs(project_root: Path) -> list[tuple[Path, Path, Path]]:
    """Default (factor json, prompt txt, output json) paths under project_root."""
    pr = project_root.resolve()
    return [
        (
            pr / "DATA/factors/latest_factor_snapshot.json",
            pr / "DATA/llm/prompts/latest_factor_snapshot.txt",
            pr / "DATA/llm/out/llm_out_latest_factor_snapshot.json",
        ),
        (
            pr / "DATA/factors/latest_factor_snapshot_withoutoficvd.json",
            pr / "DATA/llm/prompts/latest_factor_snapshot_withoutoficvd.txt",
            pr / "DATA/llm/out/llm_out_latest_factor_snapshot_withoutoficvd.json",
        ),
    ]


def _default_deepseek_chat(user_content: str) -> tuple[str, dict[str, Any]]:
    key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if not key:
        msg = "DEEPSEEK_API_KEY is not set"
        raise OSError(msg)
    base = (os.environ.get("DEEPSEEK_BASE_URL") or DEFAULT_BASE_URL).strip()
    model = (os.environ.get("DEEPSEEK_MODEL") or DEFAULT_MODEL).strip()
    return post_chat_completions(
        base_url=base,
        api_key=key,
        model=model,
        user_content=user_content,
    )


def _error_document(
    *,
    project_root: Path,
    factor_path: Path,
    factor_obj: dict[str, Any] | None,
    prompt_path: Path,
    model: str,
    generated_at: str,
    message: str,
) -> LlmAssistDocument:
    fac = factor_obj or {}
    return LlmAssistDocument(
        generated_at=generated_at,
        input_factor_path=_rel_or_abs(project_root, factor_path),
        input_factor_generated_at=str(fac.get("generated_at", "")),
        input_factor_source=str(fac.get("source", "")),
        prompt_file=_rel_or_abs(project_root, prompt_path),
        model=model,
        status="error",
        error_message=message[:2000],
        count=0,
        decisions=[],
        raw_usage={},
    )


def _factor_subset_for_prompt(factor_obj: dict[str, Any], max_factor_items: int | None) -> dict[str, Any]:
    """Shrink items sent to the LLM; full file is still read from disk and validated."""
    if max_factor_items is None or max_factor_items <= 0:
        return factor_obj
    items = factor_obj.get("items")
    if not isinstance(items, list):
        return factor_obj
    sliced = items[:max_factor_items]
    out = {**factor_obj, "items": sliced, "count": len(sliced)}
    return out


def run_llm_factor_assist_one(
    *,
    project_root: Path,
    factor_path: Path,
    prompt_path: Path,
    output_path: Path,
    chat_fn: ChatFn | None = None,
    max_factor_items: int | None = None,
) -> None:
    gen_at = to_iso_z(utc_now())
    model = (os.environ.get("DEEPSEEK_MODEL") or DEFAULT_MODEL).strip()
    fn: ChatFn = chat_fn if chat_fn is not None else _default_deepseek_chat

    factor_obj = read_json_object(factor_path)
    if not isinstance(factor_obj, dict):
        msg = "factor file root must be object"
        raise ValueError(msg)
    _validate_factor_shapes(factor_obj)

    factor_for_prompt = _factor_subset_for_prompt(factor_obj, max_factor_items)
    template = load_prompt_template(prompt_path)
    user_content = render_prompt(template, factor_for_prompt)
    if max_factor_items is not None and max_factor_items > 0:
        n = len(factor_for_prompt.get("items") or [])
        user_content += (
            "\n\nNote: The factor JSON above includes only the first "
            f"{n} item(s). "
            "Output exactly that many decisions, one per symbol in items, same order.\n"
        )
    assistant_text, usage = fn(user_content)
    rows = parse_decisions_payload(assistant_text)
    decisions = [LlmAssistDecisionItem.model_validate(r) for r in rows]
    doc = LlmAssistDocument(
        generated_at=gen_at,
        input_factor_path=_rel_or_abs(project_root, factor_path),
        input_factor_generated_at=str(factor_obj.get("generated_at", "")),
        input_factor_source=str(factor_obj.get("source", "")),
        prompt_file=_rel_or_abs(project_root, prompt_path),
        model=model,
        status="ok",
        error_message="",
        count=len(decisions),
        decisions=decisions,
        raw_usage=usage,
    )
    atomic_write_llm_assist(output_path, doc)
    log.info(
        "llm assist ok count=%s out=%s factor=%s",
        doc.count,
        output_path,
        factor_path.name,
    )


def run_llm_factor_assist_one_safe(
    *,
    project_root: Path | None = None,
    factor_path: Path | None = None,
    prompt_path: Path | None = None,
    output_path: Path | None = None,
    chat_fn: ChatFn | None = None,
    max_factor_items: int | None = None,
) -> int:
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    fp = factor_path.resolve() if factor_path else (pr / "DATA/factors/latest_factor_snapshot.json").resolve()
    pp = (
        prompt_path.resolve()
        if prompt_path
        else (pr / "DATA/llm/prompts/latest_factor_snapshot.txt").resolve()
    )
    op = (
        output_path.resolve()
        if output_path
        else (pr / "DATA/llm/out/llm_out_latest_factor_snapshot.json").resolve()
    )
    gen_at = to_iso_z(utc_now())
    model = (os.environ.get("DEEPSEEK_MODEL") or DEFAULT_MODEL).strip()
    factor_obj: dict[str, Any] | None = None
    try:
        factor_obj = read_json_object(fp)
        if not isinstance(factor_obj, dict):
            raise ValueError("factor root not object")
        _validate_factor_shapes(factor_obj)
        run_llm_factor_assist_one(
            project_root=pr,
            factor_path=fp,
            prompt_path=pp,
            output_path=op,
            chat_fn=chat_fn,
            max_factor_items=max_factor_items,
        )
    except OSError as exc:
        if "DEEPSEEK_API_KEY" in str(exc):
            log.error("llm assist config: %s", exc)
            atomic_write_llm_assist(
                op,
                _error_document(
                    project_root=pr,
                    factor_path=fp,
                    factor_obj=factor_obj if isinstance(factor_obj, dict) else None,
                    prompt_path=pp,
                    model=model,
                    generated_at=gen_at,
                    message=str(exc),
                ),
            )
            return EXIT_CONFIG
        log.exception("llm assist io: %s", exc)
        atomic_write_llm_assist(
            op,
            _error_document(
                project_root=pr,
                factor_path=fp,
                factor_obj=factor_obj if isinstance(factor_obj, dict) else None,
                prompt_path=pp,
                model=model,
                generated_at=gen_at,
                message=str(exc),
            ),
        )
        return EXIT_INTERNAL
    except (ValueError, TypeError) as exc:
        log.exception("llm assist validation: %s", exc)
        atomic_write_llm_assist(
            op,
            _error_document(
                project_root=pr,
                factor_path=fp,
                factor_obj=factor_obj if isinstance(factor_obj, dict) else None,
                prompt_path=pp,
                model=model,
                generated_at=gen_at,
                message=str(exc),
            ),
        )
        return EXIT_INTERNAL
    except Exception as exc:
        log.exception("llm assist failed: %s", exc)
        atomic_write_llm_assist(
            op,
            _error_document(
                project_root=pr,
                factor_path=fp,
                factor_obj=factor_obj if isinstance(factor_obj, dict) else None,
                prompt_path=pp,
                model=model,
                generated_at=gen_at,
                message=str(exc),
            ),
        )
        return EXIT_INTERNAL
    return EXIT_SUCCESS


def run_llm_factor_assist_twice_safe(
    *,
    project_root: Path | None = None,
    chat_fn: ChatFn | None = None,
    stdout_json: bool = False,
    pairs: list[tuple[Path, Path, Path]] | None = None,
    max_factor_items: int | None = None,
) -> int:
    """Full snapshot + without-ofi-cvd snapshot; each uses its own prompt .txt."""
    pr = project_root.resolve() if project_root else Path.cwd().resolve()
    use_pairs = pairs if pairs is not None else default_factor_assist_pairs(pr)
    worst = EXIT_SUCCESS
    summaries: list[dict[str, Any]] = []
    for fp, prompt_p, out_p in use_pairs:
        rc = run_llm_factor_assist_one_safe(
            project_root=pr,
            factor_path=fp,
            prompt_path=prompt_p,
            output_path=out_p,
            chat_fn=chat_fn,
            max_factor_items=max_factor_items,
        )
        try:
            out_display = out_p.resolve().relative_to(pr.resolve()).as_posix()
        except ValueError:
            out_display = str(out_p.resolve())
        summaries.append({"factor": fp.name, "output": out_display, "exit": rc})
        worst = max(worst, rc)

    if stdout_json:
        line = json.dumps(
            {"schema_version": "1.0", "source": "llm_factor_assist_batch", "results": summaries},
            ensure_ascii=False,
        )
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stdout.write(line + "\n")
    return worst
