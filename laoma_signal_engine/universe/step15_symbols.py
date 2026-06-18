"""How Step 1.5 must read CANDIDATE_UNIVERSE (do not use count==total_pairs as perp count)."""

from __future__ import annotations

from laoma_signal_engine.core.models import CandidateUniverseDocument, UniversePairRow


def futures_symbols_for_step_1_5(doc: CandidateUniverseDocument) -> list[str]:
    """
    Step 1.5 input: only rows with both USDT-M perp and trade-analysis eligibility.

    Equivalent to:
        [p.futures_symbol for p in doc.pairs if p.has_um_futures and p.eligible_for_trade_analysis]
    with None symbols skipped.
    """
    out: list[str] = []
    for p in doc.pairs:
        if not p.has_um_futures or not p.eligible_for_trade_analysis:
            continue
        if p.futures_symbol:
            out.append(p.futures_symbol)
    return out


def pairs_for_step_1_5(doc: CandidateUniverseDocument) -> list[UniversePairRow]:
    """Rows that Step 1.5 should process (same filter as futures_symbols_for_step_1_5)."""
    return [p for p in doc.pairs if p.has_um_futures and p.eligible_for_trade_analysis]
