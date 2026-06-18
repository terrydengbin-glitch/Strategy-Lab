"""Experiment archive and reset helpers for the P14 paper ledger."""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any

import yaml

from laoma_signal_engine.paper.candles import fetch_binance_1m_candles
from laoma_signal_engine.paper.fill_model import adverse_fill_price, build_fill_cost, paper_pnl
from laoma_signal_engine.paper.models import STRATEGY_LINES, PaperConfig
from laoma_signal_engine.paper.storage import PaperStore
from laoma_signal_engine.paper.utils import atomic_write_json, json_loads, utc_now_iso


ARCHIVE_SCHEMA_VERSION = "14.14"
RESET_TABLES = (
    "paper_trade_plans",
    "paper_orders",
    "paper_positions",
    "paper_fills",
    "paper_consumed_plans",
    "paper_performance_snapshots",
    "paper_intent_inbox",
)


def archive_reset_strategy(
    project_root: Path,
    *,
    strategy_line: str,
    profile_name: str | None = None,
    notes: str | None = None,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    """Archive one strategy ledger, force-close its open positions, then clear only that line."""

    cfg = config or PaperConfig()
    if not cfg.archive_enabled:
        raise ValueError("paper_archive_disabled")
    if strategy_line not in STRATEGY_LINES:
        raise ValueError("invalid_strategy_line")

    root = project_root.resolve()
    store = PaperStore(root, cfg)
    store.initialize()

    now = utc_now_iso()
    experiment_id = f"paper_exp_{now.replace('-', '').replace(':', '').replace('.', '_')}_{strategy_line}"
    archive_dir = _resolve(root, cfg.archive_dir) / experiment_id
    archive_dir.mkdir(parents=True, exist_ok=False)

    stats_before_force_close = store.stats()["by_line"][strategy_line]
    open_before = [row for row in store.open_positions() if row.get("strategy_line") == strategy_line]
    forced_close_rows = _force_close_line_positions(store, strategy_line, cfg, now)
    summary_before_reset = store.write_summary()
    stats_before_reset = store.stats()["by_line"][strategy_line]

    db_backup_path = archive_dir / "paper_trading.db"
    if store.db_path.exists():
        shutil.copy2(store.db_path, db_backup_path)
    summary_snapshot_path = archive_dir / "latest_paper_state.json"
    atomic_write_json(summary_snapshot_path, summary_before_reset)
    config_snapshot_path = archive_dir / "config_snapshot.json"
    atomic_write_json(config_snapshot_path, _config_snapshot(root))

    line_payload_paths = _write_line_payloads(store, archive_dir, strategy_line)
    reset_counts = _reset_strategy_line(store, strategy_line, cfg, now)
    reset_epoch = store.record_reset_epoch(
        strategy_line,
        reset_epoch_id=f"paper_epoch_{now.replace('-', '').replace(':', '').replace('.', '_')}_{strategy_line}",
        experiment_id=experiment_id,
        reset_at=now,
        reset_after_run_id=_latest_source_run_id(summary_before_reset, strategy_line),
        detail={"reset_counts": reset_counts, "open_before": len(open_before)},
    )
    summary_after_reset = store.write_summary()

    metadata = {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "strategy_line": strategy_line,
        "profile_name": profile_name or "",
        "notes": notes or "",
        "archived_at": now,
        "open_position_policy": "force_close_before_archive",
        "forced_close_exit_reason": cfg.archive_force_close_exit_reason,
        "forced_closed_positions": len(forced_close_rows),
        "open_positions_before_force_close": len(open_before),
        "stats_before_force_close": stats_before_force_close,
        "stats_before_reset": stats_before_reset,
        "stats_after_reset": summary_after_reset["stats"]["by_line"][strategy_line],
        "reset_counts": reset_counts,
        "reset_epoch": reset_epoch,
        "paths": {
            "archive_dir": _project_rel(root, archive_dir),
            "db_backup_path": _project_rel(root, db_backup_path),
            "summary_snapshot_path": _project_rel(root, summary_snapshot_path),
            "config_snapshot_path": _project_rel(root, config_snapshot_path),
            **line_payload_paths,
        },
        "forced_close_rows": forced_close_rows,
        "other_strategy_lines_untouched": [line for line in STRATEGY_LINES if line != strategy_line],
    }
    atomic_write_json(archive_dir / "metadata.json", metadata)
    _append_metadata(root, cfg, metadata)

    return {
        "status": "archived_and_reset",
        "experiment": metadata,
        "summary_after_reset": summary_after_reset,
    }


def list_experiments(project_root: Path, *, config: PaperConfig | None = None, line: str | None = None, limit: int = 50) -> dict[str, Any]:
    cfg = config or PaperConfig()
    if line is not None and line not in STRATEGY_LINES:
        raise ValueError("invalid_strategy_line")
    payload = _read_metadata(project_root.resolve(), cfg)
    experiments = payload.get("experiments") if isinstance(payload, dict) else []
    if line:
        experiments = [row for row in experiments if row.get("strategy_line") == line]
    experiments = sorted(experiments, key=lambda row: str(row.get("archived_at") or ""), reverse=True)
    return {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "count": len(experiments),
        "experiments": experiments[: max(1, int(limit or 50))],
    }


def get_experiment(project_root: Path, experiment_id: str, *, config: PaperConfig | None = None) -> dict[str, Any]:
    cfg = config or PaperConfig()
    root = project_root.resolve()
    metadata = next((row for row in _read_metadata(root, cfg).get("experiments", []) if row.get("experiment_id") == experiment_id), None)
    if not metadata:
        raise FileNotFoundError(experiment_id)
    archive_dir = _resolve(root, cfg.archive_dir) / experiment_id
    summary_path = archive_dir / "latest_paper_state.json"
    detail = {"metadata": metadata, "summary": {}}
    if summary_path.exists():
        detail["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
    for name in ("orders", "positions", "fills"):
        path = archive_dir / f"{name}.json"
        detail[name] = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    return detail


def _force_close_line_positions(store: PaperStore, line: str, cfg: PaperConfig, now: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in [row for row in store.open_positions() if row.get("strategy_line") == line]:
        reference_price, candle_ms, price_source = _archive_reference_price(position)
        quantity = float(position.get("remaining_quantity") or position.get("quantity") or 0)
        fill_price = adverse_fill_price(reference_price, str(position.get("side") or ""), "archive_reset_forced_close", cfg.default_slippage_bps)
        cost = build_fill_cost(
            reference_price=reference_price,
            fill_price=fill_price,
            side=str(position.get("side") or ""),
            action="archive_reset_forced_close",
            quantity=quantity,
            fee_bps=cfg.taker_fee_bps,
            slippage_bps=cfg.default_slippage_bps,
            cost_source=f"archive_reset:{price_source}",
        )
        gross = paper_pnl(str(position.get("side") or ""), float(position.get("entry_price") or 0), cost.fill_price, quantity)
        store.close_position(position, cost, gross_pnl=gross, exit_reason=cfg.archive_force_close_exit_reason, at=now, candle_ms=candle_ms)
        rows.append(
            {
                "position_id": position.get("id"),
                "order_id": position.get("order_id"),
                "symbol": position.get("symbol"),
                "side": position.get("side"),
                "quantity": quantity,
                "reference_price": reference_price,
                "fill_price": cost.fill_price,
                "gross_pnl_usdt": gross,
                "fee_usdt": cost.fee_usdt,
                "price_source": price_source,
            }
        )
    return rows


def _archive_reference_price(position: dict[str, Any]) -> tuple[float, int | None, str]:
    symbol = str(position.get("symbol") or "")
    try:
        candles = fetch_binance_1m_candles(symbol, limit=1)
        if candles:
            return float(candles[-1].close), int(candles[-1].open_time_ms), "binance_1m_close"
    except Exception:
        pass
    return float(position.get("entry_price") or 0), None, "entry_price_fallback"


def _write_line_payloads(store: PaperStore, archive_dir: Path, line: str) -> dict[str, str]:
    paths: dict[str, str] = {}
    for table, filename in (
        ("paper_orders", "orders.json"),
        ("paper_positions", "positions.json"),
        ("paper_fills", "fills.json"),
        ("paper_trade_plans", "trade_plans.json"),
    ):
        rows = store.row_dicts(table, line=line, limit=10000)
        path = archive_dir / filename
        atomic_write_json(path, rows)
        paths[filename.removesuffix(".json") + "_path"] = _project_rel(store.project_root, path)
    return paths


def _reset_strategy_line(store: PaperStore, line: str, cfg: PaperConfig, now: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    with store.connect() as conn:
        for table in RESET_TABLES:
            row = conn.execute(f"SELECT count(*) AS c FROM {table} WHERE strategy_line=?", (line,)).fetchone()
            counts[table] = int(row["c"] if row else 0)
            conn.execute(f"DELETE FROM {table} WHERE strategy_line=?", (line,))
        conn.execute(
            """
            INSERT INTO paper_accounts(
              strategy_line, initial_equity_usdt, equity_usdt, realized_pnl_usdt,
              unrealized_pnl_usdt, fee_usdt, slippage_usdt, updated_at
            ) VALUES(?, ?, ?, 0, 0, 0, 0, ?)
            ON CONFLICT(strategy_line) DO UPDATE SET
              initial_equity_usdt=excluded.initial_equity_usdt,
              equity_usdt=excluded.equity_usdt,
              realized_pnl_usdt=0,
              unrealized_pnl_usdt=0,
              fee_usdt=0,
              slippage_usdt=0,
              updated_at=excluded.updated_at
            """,
            (line, cfg.default_account_equity_usdt, cfg.default_account_equity_usdt, now),
        )
        _clean_worker_skipped(conn, line)
    return counts


def _clean_worker_skipped(conn: sqlite3.Connection, line: str) -> None:
    row = conn.execute("SELECT value_json FROM paper_worker_status WHERE key='daemon'").fetchone()
    if not row:
        return
    payload = json_loads(row["value_json"], {})
    last_consume = payload.get("last_consume") if isinstance(payload, dict) else None
    if isinstance(last_consume, dict) and isinstance(last_consume.get("skipped"), list):
        last_consume["skipped"] = [
            item
            for item in last_consume["skipped"]
            if (item.get("strategy_line") if isinstance(item, dict) else None) != line and (item.get("line") if isinstance(item, dict) else None) != line
        ]
        conn.execute(
            """
            INSERT INTO paper_worker_status(key, value_json, updated_at)
            VALUES('daemon', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
            """,
            (json.dumps(payload, ensure_ascii=False, separators=(",", ":")), utc_now_iso()),
        )


def _latest_source_run_id(summary: dict[str, Any], line: str) -> str | None:
    candidates: list[str] = []
    if isinstance(summary, dict):
        for bucket in ("orders", "positions", "closed_orders", "settled_positions"):
            rows = ((summary.get(bucket) or {}).get(line) or []) if isinstance(summary.get(bucket), dict) else []
            for row in rows:
                if isinstance(row, dict) and row.get("source_run_id"):
                    candidates.append(str(row.get("source_run_id")))
    return candidates[0] if candidates else None


def _append_metadata(root: Path, cfg: PaperConfig, row: dict[str, Any]) -> None:
    path = _resolve(root, cfg.archive_metadata_path)
    payload = _read_metadata(root, cfg)
    experiments = payload.get("experiments") if isinstance(payload, dict) else []
    experiments = [item for item in experiments if item.get("experiment_id") != row["experiment_id"]]
    experiments.insert(0, row)
    atomic_write_json(path, {"schema_version": ARCHIVE_SCHEMA_VERSION, "experiments": experiments})


def _read_metadata(root: Path, cfg: PaperConfig) -> dict[str, Any]:
    path = _resolve(root, cfg.archive_metadata_path)
    if not path.exists():
        return {"schema_version": ARCHIVE_SCHEMA_VERSION, "experiments": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema_version": ARCHIVE_SCHEMA_VERSION, "experiments": []}
    return payload if isinstance(payload, dict) else {"schema_version": ARCHIVE_SCHEMA_VERSION, "experiments": []}


def _config_snapshot(root: Path) -> dict[str, Any]:
    path = root / "laoma_signal_engine" / "config" / "default.yaml"
    if not path.exists():
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError):
        return {}


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _project_rel(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve())).replace("\\", "/")
    except ValueError:
        return str(path)
