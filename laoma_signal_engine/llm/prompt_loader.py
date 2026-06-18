"""Load UTF-8 prompt .txt and inject factor JSON placeholder."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PLACEHOLDER = "{{FACTOR_JSON}}"
_NO_PH_PREFIX = "---BEGIN_FACTOR_JSON---\n"


def load_prompt_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def render_prompt(template: str, factor_obj: dict[str, Any]) -> str:
    payload = json.dumps(factor_obj, ensure_ascii=False)
    if PLACEHOLDER in template:
        return template.replace(PLACEHOLDER, payload)
    return template.rstrip() + "\n\n" + _NO_PH_PREFIX + payload
