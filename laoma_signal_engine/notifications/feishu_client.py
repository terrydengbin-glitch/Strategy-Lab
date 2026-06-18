from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any

import httpx

from laoma_signal_engine.notifications.config import FeishuConfig


def build_feishu_sign(timestamp: int | str, secret: str) -> str:
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), b"", digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def build_interactive_payload(card: dict[str, Any], config: FeishuConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {"msg_type": "interactive", "card": card}
    if config.webhook_secret:
        timestamp = int(time.time())
        payload["timestamp"] = str(timestamp)
        payload["sign"] = build_feishu_sign(timestamp, config.webhook_secret)
    return payload


def send_interactive_card(
    card: dict[str, Any],
    config: FeishuConfig,
    *,
    mock: bool = False,
    force_enabled: bool = False,
) -> dict[str, Any]:
    payload = build_interactive_payload(card, config)
    base = {
        "ok": False,
        "enabled": config.enabled or force_enabled,
        "configured": config.configured,
        "sent": False,
        "status_code": None,
        "response": None,
        "error": None,
        "payload": payload if mock else None,
    }
    if mock:
        return base | {"ok": True, "sent": False, "status": "mock_sent"}
    if not config.configured:
        return base | {"error": "feishu webhook not configured"}
    if not (config.enabled or force_enabled):
        return base | {"ok": True, "error": "feishu bot disabled", "status": "disabled"}
    try:
        with httpx.Client(timeout=config.timeout_sec) as client:
            response = client.post(config.webhook_url, json=payload)
        try:
            body: Any = response.json()
        except ValueError:
            body = {"text": response.text[:300]}
        ok = response.is_success and _feishu_body_ok(body)
        return base | {
            "ok": ok,
            "sent": True,
            "status_code": response.status_code,
            "response": _summarize_response(body),
            "error": None if ok else "feishu webhook returned failure",
        }
    except Exception as exc:  # noqa: BLE001 - notification failures are non-blocking
        return base | {"error": str(exc) or repr(exc)}


def _feishu_body_ok(body: Any) -> bool:
    if not isinstance(body, dict):
        return True
    code = body.get("code", body.get("StatusCode", 0))
    return code in (0, "0", None)


def _summarize_response(body: Any) -> Any:
    if not isinstance(body, dict):
        return body
    allowed = {"code", "msg", "StatusCode", "StatusMessage", "Extra"}
    return {key: value for key, value in body.items() if key in allowed}

