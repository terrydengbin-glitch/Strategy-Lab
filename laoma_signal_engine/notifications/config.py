from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from laoma_signal_engine.core.config_loader import package_root


STRATEGY_DISPLAY_NAMES = {
    "without_micro": "异动壹号",
    "micro_fast": "异动贰号",
    "micro_full": "异动叁号",
    "strategy4": "异动肆号",
}
STRATEGY_LINES = tuple(STRATEGY_DISPLAY_NAMES)


@dataclass(frozen=True)
class FeishuConfig:
    enabled: bool = False
    webhook_url: str = ""
    webhook_secret: str = ""
    keyword: str = ""
    message_mode: str = "interactive_card"
    notify_trade_plan: bool = True
    notify_paper_order: bool = False
    notify_pipeline_summary: bool = False
    notify_audit_failure: bool = False
    notify_lines: tuple[str, ...] = STRATEGY_LINES
    strategy_display_names: dict[str, str] = field(default_factory=lambda: dict(STRATEGY_DISPLAY_NAMES))
    executable_only: bool = True
    include_non_executable_opportunities: bool = False
    block_pipeline_on_partial_failure: bool = False
    block_pipeline_on_total_failure: bool = False
    timeout_sec: float = 4.0

    @property
    def configured(self) -> bool:
        return bool(self.webhook_url)

    def strategy_name(self, line: str) -> str:
        return self.strategy_display_names.get(line) or STRATEGY_DISPLAY_NAMES.get(line) or line

    def public_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured": self.configured,
            "webhook_url": mask_secret(self.webhook_url),
            "webhook_secret": mask_secret(self.webhook_secret),
            "keyword": self.keyword,
            "message_mode": self.message_mode,
            "notify_trade_plan": self.notify_trade_plan,
            "notify_paper_order": self.notify_paper_order,
            "notify_pipeline_summary": self.notify_pipeline_summary,
            "notify_audit_failure": self.notify_audit_failure,
            "notify_lines": list(self.notify_lines),
            "strategy_display_names": self.strategy_display_names,
            "executable_only": self.executable_only,
            "include_non_executable_opportunities": self.include_non_executable_opportunities,
            "block_pipeline_on_partial_failure": self.block_pipeline_on_partial_failure,
            "block_pipeline_on_total_failure": self.block_pipeline_on_total_failure,
            "timeout_sec": self.timeout_sec,
        }


def load_feishu_config(project_root: Path | None = None, yaml_path: Path | None = None) -> FeishuConfig:
    root = project_root.resolve() if project_root else Path.cwd().resolve()
    load_dotenv(root / ".env", override=False)
    doc = _load_yaml(yaml_path or package_root() / "config" / "default.yaml")
    raw = doc.get("feishu") if isinstance(doc.get("feishu"), dict) else {}
    names = dict(STRATEGY_DISPLAY_NAMES)
    if isinstance(raw.get("strategy_display_names"), dict):
        names.update({str(k): str(v) for k, v in raw["strategy_display_names"].items()})
    lines = raw.get("notify_lines") if isinstance(raw.get("notify_lines"), list) else list(STRATEGY_LINES)
    notify_lines = tuple(line for line in (str(x) for x in lines) if line in STRATEGY_LINES)
    return FeishuConfig(
        enabled=_env_bool("FEISHU_BOT_ENABLED", raw.get("enabled", False)),
        webhook_url=str(os.getenv("FEISHU_WEBHOOK_URL") or raw.get("webhook_url") or "").strip(),
        webhook_secret=str(os.getenv("FEISHU_WEBHOOK_SECRET") or raw.get("webhook_secret") or "").strip(),
        keyword=str(os.getenv("FEISHU_WEBHOOK_KEYWORD") or raw.get("keyword") or "").strip(),
        message_mode="interactive_card",
        notify_trade_plan=bool(raw.get("notify_trade_plan", True)),
        notify_paper_order=bool(raw.get("notify_paper_order", False)),
        notify_pipeline_summary=bool(raw.get("notify_pipeline_summary", False)),
        notify_audit_failure=bool(raw.get("notify_audit_failure", False)),
        notify_lines=notify_lines or STRATEGY_LINES,
        strategy_display_names=names,
        executable_only=True,
        include_non_executable_opportunities=False,
        block_pipeline_on_partial_failure=bool(raw.get("block_pipeline_on_partial_failure", False)),
        block_pipeline_on_total_failure=bool(raw.get("block_pipeline_on_total_failure", False)),
        timeout_sec=float(raw.get("timeout_sec", 4)),
    )


def mask_secret(value: str) -> str:
    if not value:
        return ""
    text = str(value)
    if len(text) <= 10:
        return "****"
    return f"{text[:8]}***{text[-4:]}"


def _env_bool(name: str, default: Any) -> bool:
    got = os.getenv(name)
    if got is None:
        return bool(default)
    return got.strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
