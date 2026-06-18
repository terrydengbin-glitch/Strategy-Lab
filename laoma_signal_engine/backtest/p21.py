from __future__ import annotations

import hashlib
import itertools
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

from laoma_signal_engine.paper.models import PaperConfig
from laoma_signal_engine.trade_quality.diagnostics import (
    _db_path,
    _query_samples,
    diagnostic_archive_packages_payload,
    ensure_diagnostic_tables,
)

SCHEMA_VERSION = "21.0"
TARGET_STRATEGY_LINES = ("without_micro", "strategy4", "strategy5", "strategy6")
P21_DB_RELATIVE = Path("DATA/backtest/p21_parameter_optimization.db")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _ratio(part: int | float, whole: int | float) -> float:
    return round(float(part) / float(whole), 8) if whole else 0.0


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 8) if values else 0.0


def _median(values: list[float]) -> float:
    return round(float(median(values)), 8) if values else 0.0


def p21_db_path(project_root: Path) -> Path:
    return project_root.resolve() / P21_DB_RELATIVE


def ensure_p21_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS p21_problem_baselines(
              baseline_id TEXT PRIMARY KEY,
              source TEXT NOT NULL,
              archive_id TEXT,
              strategy_line TEXT NOT NULL,
              sample_count INTEGER NOT NULL,
              metrics_json TEXT NOT NULL,
              dimensions_json TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_experiments(
              experiment_id TEXT PRIMARY KEY,
              source TEXT NOT NULL,
              archive_id TEXT,
              strategy_line TEXT NOT NULL,
              sample_count INTEGER NOT NULL,
              parameter_set_count INTEGER NOT NULL,
              best_parameter_set_id TEXT,
              best_profit_factor REAL,
              best_expectancy_R REAL,
              status TEXT NOT NULL,
              schema_version TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_parameter_sets(
              parameter_set_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameters_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_backtest_results(
              result_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              reasons_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_strategy_line_results(
              result_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              strategy_line TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p21_recommendations(
              recommendation_id TEXT PRIMARY KEY,
              experiment_id TEXT NOT NULL,
              parameter_set_id TEXT NOT NULL,
              status TEXT NOT NULL,
              priority INTEGER NOT NULL,
              summary TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              parameters_json TEXT NOT NULL,
              risks_json TEXT NOT NULL,
              generated_at TEXT NOT NULL
            );
            """
        )


def _sample_db_path(project_root: Path, config: PaperConfig | None = None) -> Path:
    db_path = _db_path(project_root, config)
    ensure_diagnostic_tables(db_path)
    return db_path


def packages_payload(project_root: Path, *, config: PaperConfig | None = None) -> dict[str, Any]:
    payload = diagnostic_archive_packages_payload(project_root, config=config)
    packages = [
        {"key": "current_paper", "source": "current_paper", "label": "current paper closed", "archive_id": None},
        {"key": "all", "source": "all", "label": "all diagnostic history", "archive_id": None},
    ]
    for item in payload.get("packages") or []:
        archive_id = item.get("archive_id") or item.get("key") or item.get("path")
        packages.append(
            {
                "key": str(archive_id),
                "source": "archive",
                "archive_id": str(archive_id),
                "label": item.get("label") or item.get("archive_id") or str(archive_id),
                "trade_count": item.get("trade_count") or item.get("sample_count"),
                "path": item.get("path"),
            }
        )
    return {"schema_version": SCHEMA_VERSION, "packages": packages, "source": payload}


def _load_samples(
    project_root: Path,
    *,
    source: str = "all",
    archive_id: str | None = None,
    strategy_line: str = "all",
    limit: int = 5000,
    config: PaperConfig | None = None,
) -> tuple[list[dict[str, Any]], int]:
    db_path = _sample_db_path(project_root, config=config)
    query_source = source
    if archive_id and source not in {"archive", "all"}:
        query_source = "archive"
    rows, total = _query_samples(
        db_path,
        limit=max(1, min(int(limit or 5000), 20000)),
        source=query_source,
        archive_id=archive_id,
        strategy_line=strategy_line,
    )
    scoped = [row for row in rows if str(row.get("strategy_line") or "") in TARGET_STRATEGY_LINES]
    if strategy_line and strategy_line != "all":
        scoped = [row for row in scoped if str(row.get("strategy_line") or "") == strategy_line]
    return [_normalize_sample(row) for row in scoped], total


def _normalize_sample(row: dict[str, Any]) -> dict[str, Any]:
    evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
    entry_features = row.get("entry_features") if isinstance(row.get("entry_features"), dict) else {}
    entry_micro = row.get("entry_microstructure") if isinstance(row.get("entry_microstructure"), dict) else {}
    market_context = row.get("entry_market_context") if isinstance(row.get("entry_market_context"), dict) else {}
    entry_v3 = row.get("entry_context_v3") if isinstance(row.get("entry_context_v3"), dict) else {}
    return {
        "diagnostic_id": row.get("diagnostic_id"),
        "trade_id": row.get("trade_id"),
        "symbol": str(row.get("symbol") or "").upper(),
        "side": str(row.get("side") or "").upper(),
        "strategy_line": str(row.get("strategy_line") or "unknown"),
        "source": row.get("source"),
        "archive_id": row.get("archive_id"),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "holding_minutes": _num(row.get("holding_minutes")),
        "net_R": _num(row.get("net_R")),
        "MFE_R": _num(row.get("MFE_R")),
        "MAE_R": _num(row.get("MAE_R")),
        "fee": _num(row.get("fee")),
        "net_pnl": _num(row.get("net_pnl")),
        "exit_reason": row.get("exit_reason") or "unknown",
        "root_cause": row.get("root_cause") or "unknown",
        "quality_tags": row.get("quality_tags") or [],
        "entry_quality_label": row.get("entry_quality_label") or entry_features.get("entry_quality_label"),
        "entry_quality_v2_label": row.get("entry_quality_v2_label") or entry_micro.get("entry_quality_v2_label"),
        "market_context_label": row.get("market_context_label") or market_context.get("market_context_label"),
        "entry_context_v3_label": row.get("entry_context_v3_label") or entry_v3.get("entry_context_v3_label"),
        "funding_regime": market_context.get("funding_regime"),
        "oi_direction": market_context.get("oi_direction"),
        "btc_alignment": market_context.get("btc_alignment"),
        "planned_RR": _num(row.get("planned_RR") or evidence.get("planned_RR")),
        "evidence": evidence,
    }


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: (str(row.get("exit_time") or row.get("entry_time") or ""), str(row.get("trade_id") or "")))
    r_values = [_num(row.get("net_R")) for row in ordered]
    wins = [value for value in r_values if value > 0]
    losses = [abs(value) for value in r_values if value < 0]
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    current_loss_streak = 0
    max_losing_streak = 0
    losing_streak_distribution: Counter[str] = Counter()
    for value in r_values:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        if value < 0:
            current_loss_streak += 1
            max_losing_streak = max(max_losing_streak, current_loss_streak)
        elif current_loss_streak:
            losing_streak_distribution[str(current_loss_streak)] += 1
            current_loss_streak = 0
    if current_loss_streak:
        losing_streak_distribution[str(current_loss_streak)] += 1
    gross_profit = round(sum(wins), 8)
    gross_loss_abs = round(sum(losses), 8)
    avg_win = _avg(wins)
    avg_loss = _avg(losses)
    return {
        "trade_count": len(r_values),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": _ratio(len(wins), len(r_values)),
        "gross_profit_R": gross_profit,
        "gross_loss_R": round(-gross_loss_abs, 8),
        "profit_factor": round(gross_profit / gross_loss_abs, 8) if gross_loss_abs else (None if gross_profit == 0 else 999.0),
        "total_R": round(sum(r_values), 8),
        "expectancy_R": _avg(r_values),
        "avg_win_R": avg_win,
        "avg_loss_R": avg_loss,
        "profit_loss_ratio": round(avg_win / avg_loss, 8) if avg_loss else None,
        "median_net_R": _median(r_values),
        "avg_MFE_R": _avg([_num(row.get("MFE_R")) for row in rows]),
        "avg_MAE_R": _avg([_num(row.get("MAE_R")) for row in rows]),
        "max_drawdown_R": round(max_drawdown, 8),
        "max_losing_streak": max_losing_streak,
        "losing_streak_distribution": dict(losing_streak_distribution),
    }


def _group_metrics(rows: list[dict[str, Any]], key: str, *, limit: int = 20) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        value = row.get(key)
        if key == "hour":
            value = str(row.get("exit_time") or row.get("entry_time") or "")[11:13] or "unknown"
        elif key == "holding_bucket":
            mins = _num(row.get("holding_minutes"))
            if mins <= 3:
                value = "0-3m"
            elif mins <= 10:
                value = "3-10m"
            elif mins <= 30:
                value = "10-30m"
            elif mins <= 60:
                value = "30-60m"
            else:
                value = "60m+"
        groups[str(value or "unknown")].append(row)
    output = []
    for value, items in groups.items():
        item = {"key": value, **_metrics(items)}
        output.append(item)
    output.sort(key=lambda item: (item["total_R"], -item["trade_count"]))
    return output[:limit]


def baseline_payload(
    project_root: Path,
    *,
    source: str = "all",
    archive_id: str | None = None,
    strategy_line: str = "all",
    limit: int = 5000,
    write: bool = True,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    rows, total_available = _load_samples(
        project_root,
        source=source,
        archive_id=archive_id,
        strategy_line=strategy_line,
        limit=limit,
        config=config,
    )
    metrics = _metrics(rows)
    dimensions = {
        "strategy_line": _group_metrics(rows, "strategy_line"),
        "symbol": _group_metrics(rows, "symbol"),
        "side": _group_metrics(rows, "side"),
        "root_cause": _group_metrics(rows, "root_cause"),
        "hour": _group_metrics(rows, "hour"),
        "holding_bucket": _group_metrics(rows, "holding_bucket"),
        "entry_context_v3_label": _group_metrics(rows, "entry_context_v3_label"),
    }
    generated_at = _now()
    baseline_id = hashlib.sha256(
        f"{source}|{archive_id}|{strategy_line}|{limit}|{generated_at}".encode("utf-8")
    ).hexdigest()[:20]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "baseline_id": baseline_id,
        "source": source,
        "archive_id": archive_id,
        "strategy_line": strategy_line,
        "sample_count": len(rows),
        "total_available": total_available,
        "metrics": metrics,
        "dimensions": dimensions,
        "generated_at": generated_at,
    }
    if write:
        db_path = p21_db_path(project_root)
        ensure_p21_tables(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO p21_problem_baselines(
                  baseline_id, source, archive_id, strategy_line, sample_count,
                  metrics_json, dimensions_json, schema_version, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    baseline_id,
                    source,
                    archive_id,
                    strategy_line,
                    len(rows),
                    _json(metrics),
                    _json(dimensions),
                    SCHEMA_VERSION,
                    generated_at,
                ),
            )
    return payload


def _default_parameter_sets(strategy_line: str = "all") -> list[dict[str, Any]]:
    lines = list(TARGET_STRATEGY_LINES if strategy_line == "all" else [strategy_line])
    root_sets = [
        [],
        ["direction_wrong"],
        ["direction_wrong", "signal_no_edge"],
        ["direction_wrong", "signal_no_edge", "tp_too_far"],
    ]
    sets: list[dict[str, Any]] = []
    for line, min_mfe, max_mae, roots, min_pf_samples in itertools.product(
        lines,
        [0.0, 0.3, 0.5],
        [999.0, 1.2, 0.8],
        root_sets,
        [0],
    ):
        params = {
            "target_strategy_line": line,
            "min_MFE_R": min_mfe,
            "max_MAE_R": max_mae,
            "blocked_root_causes": roots,
            "min_samples_per_symbol": min_pf_samples,
            "mode": "shadow_filter",
        }
        param_id = hashlib.sha256(_json(params).encode("utf-8")).hexdigest()[:16]
        sets.append({"parameter_set_id": f"p21_{param_id}", "parameters": params})
    return sets


def _apply_parameter_set(row: dict[str, Any], params: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    target_line = params.get("target_strategy_line") or "all"
    if target_line != "all" and row.get("strategy_line") != target_line:
        return False, ["outside_strategy_line"]
    if _num(row.get("MFE_R")) < _num(params.get("min_MFE_R")):
        reasons.append("low_MFE_R")
    max_mae = _num(params.get("max_MAE_R"), 999.0)
    if max_mae < 900 and _num(row.get("MAE_R")) > max_mae:
        reasons.append("high_MAE_R")
    blocked_roots = set(params.get("blocked_root_causes") or [])
    if str(row.get("root_cause") or "") in blocked_roots:
        reasons.append("blocked_root_cause")
    return not reasons, reasons


def run_matrix_payload(
    project_root: Path,
    *,
    source: str = "all",
    archive_id: str | None = None,
    strategy_line: str = "all",
    limit: int = 5000,
    max_sets: int = 120,
    parameter_grid: list[dict[str, Any]] | None = None,
    write: bool = True,
    config: PaperConfig | None = None,
) -> dict[str, Any]:
    rows, _ = _load_samples(project_root, source=source, archive_id=archive_id, strategy_line=strategy_line, limit=limit, config=config)
    generated_at = _now()
    experiment_id = hashlib.sha256(
        f"{source}|{archive_id}|{strategy_line}|{limit}|{generated_at}".encode("utf-8")
    ).hexdigest()[:20]
    parameter_sets = parameter_grid or _default_parameter_sets(strategy_line=strategy_line)
    parameter_sets = parameter_sets[: max(1, min(int(max_sets or 120), 500))]
    leaderboard: list[dict[str, Any]] = []
    line_results: list[dict[str, Any]] = []
    for item in parameter_sets:
        params = dict(item.get("parameters") or item)
        parameter_set_id = item.get("parameter_set_id") or hashlib.sha256(_json(params).encode("utf-8")).hexdigest()[:16]
        accepted: list[dict[str, Any]] = []
        reason_counter: Counter[str] = Counter()
        for row in rows:
            ok, reasons = _apply_parameter_set(row, params)
            if ok:
                accepted.append(row)
            else:
                reason_counter.update(reasons)
        metrics = _metrics(accepted)
        metrics.update(
            {
                "input_sample_count": len(rows),
                "accepted_count": len(accepted),
                "blocked_count": len(rows) - len(accepted),
                "acceptance_rate": _ratio(len(accepted), len(rows)),
            }
        )
        result = {
            "experiment_id": experiment_id,
            "parameter_set_id": parameter_set_id,
            "parameters": params,
            "metrics": metrics,
            "reasons": dict(reason_counter),
        }
        leaderboard.append(result)
        for line in TARGET_STRATEGY_LINES:
            line_rows = [row for row in accepted if row.get("strategy_line") == line]
            line_results.append(
                {
                    "experiment_id": experiment_id,
                    "parameter_set_id": parameter_set_id,
                    "strategy_line": line,
                    "metrics": _metrics(line_rows) | {"accepted_count": len(line_rows)},
                }
            )
    leaderboard.sort(
        key=lambda item: (
            item["metrics"].get("profit_factor") if item["metrics"].get("profit_factor") is not None else -999,
            item["metrics"].get("expectancy_R") or -999,
            -item["metrics"].get("max_drawdown_R", 0),
        ),
        reverse=True,
    )
    best = leaderboard[0] if leaderboard else None
    recommendations = _build_recommendations(experiment_id, leaderboard)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "source": source,
        "archive_id": archive_id,
        "strategy_line": strategy_line,
        "sample_count": len(rows),
        "parameter_set_count": len(parameter_sets),
        "leaderboard": leaderboard[:50],
        "strategy_line_results": line_results,
        "recommendations": recommendations,
        "best": best,
        "generated_at": generated_at,
    }
    if write:
        _persist_matrix(project_root, payload)
    return payload


def _build_recommendations(experiment_id: str, leaderboard: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for index, item in enumerate(leaderboard[:10], start=1):
        metrics = item["metrics"]
        pf = metrics.get("profit_factor")
        status = "candidate_pf_gt_1" if pf and pf > 1 else "watch_only"
        risks = []
        if metrics.get("accepted_count", 0) < 10:
            risks.append("small_sample")
        if metrics.get("acceptance_rate", 0) < 0.1:
            risks.append("over_filtered")
        output.append(
            {
                "recommendation_id": hashlib.sha256(
                    f"{experiment_id}|{item['parameter_set_id']}|{index}".encode("utf-8")
                ).hexdigest()[:20],
                "experiment_id": experiment_id,
                "parameter_set_id": item["parameter_set_id"],
                "status": status,
                "priority": index,
                "summary": f"PF={pf} expectancy={metrics.get('expectancy_R')} accepted={metrics.get('accepted_count')}",
                "metrics": metrics,
                "parameters": item["parameters"],
                "risks": risks,
            }
        )
    return output


def _persist_matrix(project_root: Path, payload: dict[str, Any]) -> None:
    db_path = p21_db_path(project_root)
    ensure_p21_tables(db_path)
    best = payload.get("best") or {}
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO p21_experiments(
              experiment_id, source, archive_id, strategy_line, sample_count, parameter_set_count,
              best_parameter_set_id, best_profit_factor, best_expectancy_R, status, schema_version, generated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["experiment_id"],
                payload["source"],
                payload.get("archive_id"),
                payload["strategy_line"],
                payload["sample_count"],
                payload["parameter_set_count"],
                best.get("parameter_set_id"),
                (best.get("metrics") or {}).get("profit_factor"),
                (best.get("metrics") or {}).get("expectancy_R"),
                "completed",
                SCHEMA_VERSION,
                payload["generated_at"],
            ),
        )
        for item in payload.get("leaderboard") or []:
            conn.execute(
                "INSERT OR REPLACE INTO p21_parameter_sets(parameter_set_id, experiment_id, parameters_json, generated_at) VALUES(?, ?, ?, ?)",
                (item["parameter_set_id"], payload["experiment_id"], _json(item["parameters"]), payload["generated_at"]),
            )
            result_id = hashlib.sha256(f"{payload['experiment_id']}|{item['parameter_set_id']}".encode("utf-8")).hexdigest()[:24]
            conn.execute(
                "INSERT OR REPLACE INTO p21_backtest_results(result_id, experiment_id, parameter_set_id, metrics_json, reasons_json, generated_at) VALUES(?, ?, ?, ?, ?, ?)",
                (result_id, payload["experiment_id"], item["parameter_set_id"], _json(item["metrics"]), _json(item["reasons"]), payload["generated_at"]),
            )
        for item in payload.get("strategy_line_results") or []:
            result_id = hashlib.sha256(
                f"{item['experiment_id']}|{item['parameter_set_id']}|{item['strategy_line']}".encode("utf-8")
            ).hexdigest()[:24]
            conn.execute(
                "INSERT OR REPLACE INTO p21_strategy_line_results(result_id, experiment_id, parameter_set_id, strategy_line, metrics_json, generated_at) VALUES(?, ?, ?, ?, ?, ?)",
                (result_id, item["experiment_id"], item["parameter_set_id"], item["strategy_line"], _json(item["metrics"]), payload["generated_at"]),
            )
        for item in payload.get("recommendations") or []:
            conn.execute(
                """
                INSERT OR REPLACE INTO p21_recommendations(
                  recommendation_id, experiment_id, parameter_set_id, status, priority, summary,
                  metrics_json, parameters_json, risks_json, generated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["recommendation_id"],
                    item["experiment_id"],
                    item["parameter_set_id"],
                    item["status"],
                    item["priority"],
                    item["summary"],
                    _json(item["metrics"]),
                    _json(item["parameters"]),
                    _json(item["risks"]),
                    payload["generated_at"],
                ),
            )


def experiments_payload(project_root: Path, *, limit: int = 50) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_experiments ORDER BY generated_at DESC LIMIT ?",
            (max(1, min(int(limit or 50), 200)),),
        ).fetchall()]
    return {"schema_version": SCHEMA_VERSION, "count": len(rows), "experiments": rows}


def experiment_detail_payload(project_root: Path, experiment_id: str) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        exp = conn.execute("SELECT * FROM p21_experiments WHERE experiment_id = ?", (experiment_id,)).fetchone()
        results = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_backtest_results WHERE experiment_id = ? ORDER BY json_extract(metrics_json, '$.profit_factor') DESC",
            (experiment_id,),
        ).fetchall()]
        recs = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_recommendations WHERE experiment_id = ? ORDER BY priority ASC",
            (experiment_id,),
        ).fetchall()]
    if not exp:
        return {"schema_version": SCHEMA_VERSION, "experiment_id": experiment_id, "found": False}
    for row in results:
        row["metrics"] = _loads(row.pop("metrics_json", None), {})
        row["reasons"] = _loads(row.pop("reasons_json", None), {})
    for row in recs:
        row["metrics"] = _loads(row.pop("metrics_json", None), {})
        row["parameters"] = _loads(row.pop("parameters_json", None), {})
        row["risks"] = _loads(row.pop("risks_json", None), [])
    return {"schema_version": SCHEMA_VERSION, "found": True, "experiment": dict(exp), "results": results, "recommendations": recs}


def recommendations_payload(project_root: Path, *, limit: int = 50) -> dict[str, Any]:
    db_path = p21_db_path(project_root)
    ensure_p21_tables(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(row) for row in conn.execute(
            "SELECT * FROM p21_recommendations ORDER BY generated_at DESC, priority ASC LIMIT ?",
            (max(1, min(int(limit or 50), 200)),),
        ).fetchall()]
    for row in rows:
        row["metrics"] = _loads(row.pop("metrics_json", None), {})
        row["parameters"] = _loads(row.pop("parameters_json", None), {})
        row["risks"] = _loads(row.pop("risks_json", None), [])
    return {"schema_version": SCHEMA_VERSION, "count": len(rows), "recommendations": rows}


def export_config_candidate_payload(project_root: Path, *, experiment_id: str, parameter_set_id: str | None = None) -> dict[str, Any]:
    detail = experiment_detail_payload(project_root, experiment_id)
    if not detail.get("found"):
        return {"status": "not_found", "experiment_id": experiment_id}
    candidates = detail.get("results") or []
    selected = None
    if parameter_set_id:
        selected = next((item for item in candidates if item.get("parameter_set_id") == parameter_set_id), None)
    if selected is None and candidates:
        selected = candidates[0]
    if selected is None:
        return {"status": "empty", "experiment_id": experiment_id}
    parameters: dict[str, Any] = {}
    db_path = p21_db_path(project_root)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT parameters_json FROM p21_parameter_sets WHERE experiment_id = ? AND parameter_set_id = ?",
            (experiment_id, selected["parameter_set_id"]),
        ).fetchone()
        if row:
            parameters = _loads(row[0], {})
    generated_at = _now()
    candidate = {
        "schema_version": SCHEMA_VERSION,
        "status": "shadow_config_candidate",
        "experiment_id": experiment_id,
        "parameter_set_id": selected["parameter_set_id"],
        "parameters": parameters,
        "metrics": selected.get("metrics") or {},
        "note": "P21 candidate only. Manual review required before runtime config changes.",
        "generated_at": generated_at,
    }
    path = project_root / "DATA/backtest/p21_latest_config_candidate.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json(candidate), encoding="utf-8")
    return {"status": "ok", "path": str(path), "candidate": candidate}
