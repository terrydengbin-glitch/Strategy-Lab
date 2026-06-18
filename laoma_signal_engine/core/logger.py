"""stderr logging (avoid logging.basicConfig at import time)."""

from __future__ import annotations

import logging
import sys


def setup_stderr_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure root logger once with a StreamHandler to stderr."""
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
        root.addHandler(handler)
        root.setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger("laoma_signal_engine")
