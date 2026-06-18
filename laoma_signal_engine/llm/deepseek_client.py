"""DeepSeek OpenAI-compatible chat API. STEP6.0."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Callable

import httpx

ChatFn = Callable[..., tuple[str, dict[str, Any]]]

DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-v4-pro"
CHAT_PATH = "/v1/chat/completions"


def _model_uses_v4_thinking_api(model: str) -> bool:
    m = model.strip().lower()
    return m.startswith("deepseek-v4") or m == "deepseek-reasoner"


def resolve_chat_timeout_sec(model: str) -> float:
    raw = (os.environ.get("DEEPSEEK_TIMEOUT_SEC") or "").strip()
    if raw:
        return float(raw)
    m = model.strip().lower()
    if m.startswith("deepseek-v4") or m == "deepseek-reasoner":
        flag = (os.environ.get("DEEPSEEK_THINKING") or "enabled").strip().lower()
        if flag in ("0", "false", "no", "off", "disabled"):
            return 120.0
        return 300.0
    return 120.0


def post_chat_completions(
    *,
    base_url: str,
    api_key: str,
    model: str,
    user_content: str,
    timeout_sec: float | None = None,
) -> tuple[str, dict[str, Any]]:
    eff_timeout = float(timeout_sec) if timeout_sec is not None else resolve_chat_timeout_sec(model)
    url = base_url.rstrip("/") + CHAT_PATH
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json; charset=utf-8",
    }
    body: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": user_content}],
    }
    if _model_uses_v4_thinking_api(model):
        flag = (os.environ.get("DEEPSEEK_THINKING") or "enabled").strip().lower()
        if flag in ("0", "false", "no", "off", "disabled"):
            body["thinking"] = {"type": "disabled"}
            body["temperature"] = 0.2
        else:
            body["thinking"] = {"type": "enabled"}
            effort = (os.environ.get("DEEPSEEK_REASONING_EFFORT") or "max").strip().lower()
            if effort not in ("high", "max"):
                effort = "max"
            body["reasoning_effort"] = effort
    else:
        body["temperature"] = 0.2
    with httpx.Client(timeout=eff_timeout) as client:
        resp = client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        msg = "deepseek response missing choices"
        raise ValueError(msg)
    msg0 = choices[0]
    if not isinstance(msg0, dict):
        msg = "deepseek choice invalid"
        raise ValueError(msg)
    message = msg0.get("message")
    if not isinstance(message, dict):
        msg = "deepseek message invalid"
        raise ValueError(msg)
    content = message.get("content")
    if not isinstance(content, str):
        msg = "deepseek content not string"
        raise ValueError(msg)
    usage = data.get("usage")
    usage_d: dict[str, Any] = usage if isinstance(usage, dict) else {}
    return content.strip(), usage_d


_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)


def extract_json_text(assistant_text: str) -> str:
    t = assistant_text.strip()
    m = _FENCE_RE.search(t)
    if m:
        return m.group(1).strip()
    return t


def parse_decisions_payload(assistant_text: str) -> list[dict[str, Any]]:
    raw = extract_json_text(assistant_text)
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = f"llm output is not valid json: {exc}"
        raise ValueError(msg) from exc
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        dec = obj.get("decisions")
        if isinstance(dec, list):
            return [x for x in dec if isinstance(x, dict)]
    msg = "llm json must be object with decisions[] or a list of objects"
    raise ValueError(msg)
