"""Exchange symbol contract helpers for downstream trade safety."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")


@dataclass(frozen=True)
class SymbolContractResult:
    raw_symbol: str
    normalized_symbol: str
    ok: bool
    reason: str = ""
    source: str = "format_only"
    whitelist_available: bool = False

    def guards(self) -> dict[str, Any]:
        return {
            "symbol_raw": self.raw_symbol,
            "symbol_normalized": self.normalized_symbol,
            "symbol_contract_ok": self.ok,
            "symbol_contract_reason": self.reason,
            "symbol_contract_source": self.source,
            "symbol_contract_whitelist_available": self.whitelist_available,
        }


def normalize_symbol(raw: Any) -> str:
    return str(raw or "").strip().upper()


def load_exchange_symbol_whitelist(project_root: Path | None) -> tuple[set[str], str, bool]:
    if project_root is None:
        return set(), "format_only", False
    root = Path(project_root).resolve()
    light_path = root / "DATA" / "market" / "futures_light_snapshot.json"
    symbols: set[str] = set()
    if light_path.is_file():
        try:
            data = json.loads(light_path.read_text(encoding="utf-8"))
            for row in data.get("items") or []:
                if isinstance(row, dict):
                    sym = normalize_symbol(row.get("symbol"))
                    if sym:
                        symbols.add(sym)
            if symbols:
                return symbols, "futures_light_snapshot", True
        except (OSError, ValueError, TypeError):
            pass

    universe_path = root / "DATA" / "universe" / "CANDIDATE_UNIVERSE.json"
    if universe_path.is_file():
        try:
            data = json.loads(universe_path.read_text(encoding="utf-8"))
            for row in data.get("pairs") or []:
                if not isinstance(row, dict):
                    continue
                if row.get("has_um_futures") is not True:
                    continue
                if row.get("eligible_for_trade_analysis") is False:
                    continue
                sym = normalize_symbol(row.get("futures_symbol") or row.get("symbol_safe_id"))
                if sym:
                    symbols.add(sym)
            if symbols:
                return symbols, "candidate_universe", True
        except (OSError, ValueError, TypeError):
            pass
    return set(), "exchange_whitelist_unavailable", False


def validate_exchange_symbol(
    raw_symbol: Any,
    *,
    project_root: Path | None = None,
    whitelist: set[str] | None = None,
    source: str | None = None,
    fail_closed_on_missing_whitelist: bool = False,
) -> SymbolContractResult:
    raw = str(raw_symbol or "")
    normalized = normalize_symbol(raw_symbol)
    if not normalized:
        return SymbolContractResult(raw, normalized, False, "symbol_missing")
    if normalized != raw.strip():
        return SymbolContractResult(raw, normalized, False, "symbol_not_normalized")
    if not SYMBOL_RE.match(normalized):
        return SymbolContractResult(raw, normalized, False, "invalid_symbol_format")

    whitelist_available = whitelist is not None
    src = source or ("explicit_whitelist" if whitelist_available else "format_only")
    got_whitelist = whitelist
    if got_whitelist is None and project_root is not None:
        got_whitelist, src, whitelist_available = load_exchange_symbol_whitelist(project_root)

    if got_whitelist:
        if normalized not in got_whitelist:
            return SymbolContractResult(raw, normalized, False, "not_in_exchange_whitelist", src, True)
        return SymbolContractResult(raw, normalized, True, "", src, True)

    if fail_closed_on_missing_whitelist and project_root is not None:
        return SymbolContractResult(raw, normalized, False, "exchange_symbol_whitelist_unavailable", src, False)
    return SymbolContractResult(raw, normalized, True, "", src, False)
