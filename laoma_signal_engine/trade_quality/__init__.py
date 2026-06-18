"""Trade quality diagnostics for paper-trade outcomes."""

from laoma_signal_engine.trade_quality.engine import (
    TradeQualityAnalyzer,
    analyze_paper_trades,
    ensure_trade_quality_tables,
)
from laoma_signal_engine.trade_quality.archive_backfill import (
    archive_backfill_payload,
    ensure_archive_ingest_tables,
    ingest_ledger_rows,
)
from laoma_signal_engine.trade_quality.replay_backfill import (
    ensure_replay_backfill_tables,
    replay_backfill_ledger_rows,
    replay_backfill_payload,
    replay_backfill_summary,
)
from laoma_signal_engine.trade_quality.recommendation_rules import (
    ensure_recommendation_rule_tables,
    recommendation_rules_payload,
    rebuild_recommendation_rules,
)
from laoma_signal_engine.trade_quality.recommendation_validation import recommendation_validation_payload
from laoma_signal_engine.trade_quality.promotion_policy import (
    apply_promotion,
    disable_promotion,
    ensure_promotion_tables,
    promotion_dry_run,
    promotions_payload,
)

__all__ = [
    "TradeQualityAnalyzer",
    "analyze_paper_trades",
    "archive_backfill_payload",
    "ensure_archive_ingest_tables",
    "ensure_trade_quality_tables",
    "ingest_ledger_rows",
    "ensure_replay_backfill_tables",
    "replay_backfill_payload",
    "replay_backfill_ledger_rows",
    "replay_backfill_summary",
    "ensure_recommendation_rule_tables",
    "recommendation_rules_payload",
    "rebuild_recommendation_rules",
    "recommendation_validation_payload",
    "ensure_promotion_tables",
    "promotions_payload",
    "promotion_dry_run",
    "apply_promotion",
    "disable_promotion",
]
