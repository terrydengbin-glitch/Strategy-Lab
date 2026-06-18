"""Post-close Trade Quality completion for paper-backed ledgers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.trade_quality.engine import analyze_paper_trades


DISABLE_ENV = "P29_TQ_AUTO_COMPLETION_DISABLED"


def complete_paper_trade_quality(
    project_root: Path,
    *,
    config: PaperConfig,
    candle_provider: Any | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Run the official Trade Quality module before P29 sidecar sync.

    This helper is intentionally best-effort: paper and paper-equivalent
    execution must not be rolled back because a training-label completion pass
    failed.  Failures are returned to the caller and later reflected in the
    sidecar readiness gate.
    """

    if os.environ.get(DISABLE_ENV) == "1":
        return {
            "trade_quality_completion_status": "disabled",
            "reason": f"{DISABLE_ENV}=1",
            "sample_count": 0,
        }
    try:
        result = analyze_paper_trades(
            Path(project_root),
            config=config,
            candle_provider=candle_provider,
            persist=True,
            limit=limit,
        )
        return {
            "trade_quality_completion_status": "ok",
            "provider": "laoma_signal_engine.trade_quality.engine",
            "module_version": result.get("schema_version"),
            "db_path": result.get("db_path"),
            "sample_count": int(result.get("sample_count") or 0),
            "generated_at": result.get("generated_at"),
        }
    except Exception as exc:  # pragma: no cover - caller must not fail trading
        return {
            "trade_quality_completion_status": "failed",
            "provider": "laoma_signal_engine.trade_quality.engine",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "sample_count": 0,
        }
