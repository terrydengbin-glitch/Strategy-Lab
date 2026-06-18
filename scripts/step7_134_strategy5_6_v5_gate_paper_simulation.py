from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from laoma_signal_engine.paper.config import load_paper_config
from laoma_signal_engine.paper.daemon import read_status as read_paper_status
from laoma_signal_engine.strategy_pipeline import run_strategy_pipeline_safe


TASK_ID = "STEP7.134"
GATE_CONFIG_REL = Path("DATA/paper/v5_trade_gate_experiment.json")
OUTPUT_JSON_REL = Path("DATA/paper/step7_134_strategy5_6_v5_gate_paper_simulation.json")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _paper_db_path(root: Path) -> Path:
    cfg = load_paper_config(root)
    raw = Path(str(cfg.db_path))
    return raw if raw.is_absolute() else root / raw


def _default_config_snapshot(root: Path) -> dict[str, Any]:
    path = root / "laoma_signal_engine" / "config" / "default.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    lines = doc.get("trade_plan_lines") if isinstance(doc.get("trade_plan_lines"), dict) else {}
    return {
        "path": str(path),
        "strategy5_parameter_set_id": ((lines.get("strategy5") or {}).get("parameter_set_id") if isinstance(lines.get("strategy5"), dict) else None),
        "strategy6_parameter_set_id": ((lines.get("strategy6") or {}).get("parameter_set_id") if isinstance(lines.get("strategy6"), dict) else None),
        "notify_trade_plan": ((doc.get("notifications") or {}).get("notify_trade_plan") if isinstance(doc.get("notifications"), dict) else None),
    }


def _validate_gate_config(config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if config.get("enabled") is not True:
        errors.append("gate_config_not_enabled")
    if str(config.get("feature_missing_policy") or "") != "block":
        errors.append("feature_missing_policy_not_block")
    rules = config.get("rules") if isinstance(config.get("rules"), dict) else {}
    expected = {
        "strategy5": "p21v2_72340cb432fa7977",
        "strategy6": "s6v32_edcd6b1030331422",
    }
    for line, param_id in expected.items():
        rule = rules.get(line) if isinstance(rules.get(line), dict) else {}
        if not rule:
            errors.append(f"{line}_rule_missing")
            continue
        if rule.get("parameter_set_id") != param_id:
            errors.append(f"{line}_parameter_set_mismatch")
        if not isinstance(rule.get("rule_json"), dict):
            errors.append(f"{line}_rule_json_missing")
        if str(rule.get("action") or "") != "block":
            errors.append(f"{line}_action_not_block")
    return errors


def _prepare_gate_config(root: Path, *, stamp: str) -> dict[str, Any]:
    path = root / GATE_CONFIG_REL
    original = _read_json(path)
    backup_path = root / "DATA" / "paper" / "gate_config_snapshots" / f"v5_trade_gate_experiment_before_STEP7.134_{stamp}.json"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        shutil.copy2(path, backup_path)
    experiment_id = f"paper_exp_step7_134_strategy5_6_v5_gate_{stamp}"
    epoch_id = f"paper_epoch_step7_134_{stamp}"
    config = dict(original)
    config.update(
        {
            "enabled": True,
            "experiment_id": experiment_id,
            "paper_epoch_id": epoch_id,
            "line_epochs": {
                "strategy5": f"{epoch_id}_strategy5",
                "strategy6": f"{epoch_id}_strategy6",
            },
            "mode": "paper_experiment",
            "feature_missing_policy": "block",
        }
    )
    rules = config.get("rules") if isinstance(config.get("rules"), dict) else {}
    config["rules"] = {
        **rules,
        "strategy5": {
            "parameter_set_id": "p21v2_72340cb432fa7977",
            "gate_candidate_id": "strategy5_v5_opposite_flow_combo_gate",
            "action": "block",
            "rule_json": {
                "operator": "AND",
                "rules": [
                    {"field": "side_flow_alignment", "op": "eq", "value": "opposite"},
                    {"field": "price_flow_alignment", "op": "eq", "value": "opposite"},
                ],
            },
            "evidence": {
                "step7_143_real_e2e_pf_before": 0.7178196,
                "step7_143_real_e2e_pf_after": 0.71884948,
            },
        },
        "strategy6": {
            "parameter_set_id": "s6v32_edcd6b1030331422",
            "gate_candidate_id": "strategy6_v5_negative_funding_short_crowded_gate",
            "action": "block",
            "rule_json": {
                "operator": "AND",
                "rules": [
                    {"field": "funding_bucket", "op": "eq", "value": "NEGATIVE_EXTREME"},
                    {"field": "funding_crowded_side", "op": "eq", "value": "short"},
                ],
            },
            "evidence": {
                "step7_143_real_e2e_pf_before": 0.72993924,
                "step7_143_real_e2e_pf_after": 0.75219144,
            },
        },
    }
    _write_json(path, config)
    return {
        "path": str(path),
        "backup_path": str(backup_path),
        "experiment_id": experiment_id,
        "paper_epoch_id": epoch_id,
        "config": config,
        "validation_errors": _validate_gate_config(config),
    }


def _connect_paper(root: Path) -> sqlite3.Connection | None:
    path = _paper_db_path(root)
    if not path.exists():
        return None
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _paper_counts(root: Path, *, experiment_id: str | None = None) -> dict[str, Any]:
    db_path = _paper_db_path(root)
    out: dict[str, Any] = {"db_path": str(db_path), "exists": db_path.exists()}
    conn = _connect_paper(root)
    if conn is None:
        return out
    try:
        for table in ("paper_orders", "paper_skip_ledger", "paper_intent_inbox", "paper_fills", "paper_positions"):
            try:
                out[f"{table}_total"] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            except sqlite3.Error as exc:
                out[f"{table}_error"] = str(exc)
        if experiment_id:
            for table in ("paper_orders", "paper_skip_ledger", "paper_intent_inbox"):
                try:
                    rows = conn.execute(
                        f"""
                        SELECT strategy_line, COALESCE(gate_decision, '') AS gate_decision,
                               COALESCE(gate_candidate_id, '') AS gate_candidate_id,
                               COUNT(*) AS n
                        FROM {table}
                        WHERE experiment_id = ?
                        GROUP BY strategy_line, gate_decision, gate_candidate_id
                        ORDER BY strategy_line, gate_decision, gate_candidate_id
                        """,
                        (experiment_id,),
                    ).fetchall()
                    out[f"{table}_by_gate"] = [dict(row) for row in rows]
                except sqlite3.Error as exc:
                    out[f"{table}_by_gate_error"] = str(exc)
            try:
                rows = conn.execute(
                    """
                    SELECT strategy_line, status, COUNT(*) AS n
                    FROM paper_orders
                    WHERE experiment_id = ?
                    GROUP BY strategy_line, status
                    ORDER BY strategy_line, status
                    """,
                    (experiment_id,),
                ).fetchall()
                out["paper_orders_by_status"] = [dict(row) for row in rows]
            except sqlite3.Error:
                pass
        try:
            rows = conn.execute(
                """
                SELECT strategy_line, status, COUNT(*) AS n
                FROM paper_positions
                WHERE status = 'open'
                GROUP BY strategy_line, status
                ORDER BY strategy_line
                """
            ).fetchall()
            out["open_positions"] = [dict(row) for row in rows]
        except sqlite3.Error:
            pass
    finally:
        conn.close()
    return out


def _latest_plan_summary(root: Path, line: str) -> dict[str, Any]:
    path = root / "DATA" / "decisions" / f"latest_trade_plan_{line}.json"
    doc = _read_json(path)
    plans = doc.get("plans") if isinstance(doc.get("plans"), list) else []
    return {
        "path": str(path),
        "exists": path.exists(),
        "source": doc.get("source"),
        "status": doc.get("status"),
        "run_id": doc.get("run_id"),
        "cycle_id": doc.get("cycle_id"),
        "generated_at": doc.get("generated_at"),
        "count": doc.get("count"),
        "executable_count": doc.get("executable_count"),
        "paper_eligible_count": sum(1 for plan in plans if isinstance(plan, dict) and plan.get("paper_eligible") is True),
    }


def _run_smoke(root: Path) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for line in ("strategy5", "strategy6"):
        started = _now()
        rc = run_strategy_pipeline_safe(
            project_root=root,
            line=line,  # type: ignore[arg-type]
            mode="once",
            max_cycles=1,
            stdout_json=False,
            force_universe=None,
            light_limit=None,
            scan_allow_stale_input=None,
            skip_market_context=False,
            skip_micro_wait=False,
            run_abc_audit=True,
            run_json_stage_audit=True,
            aggregate_final_decisions=True,
        )
        results[line] = {
            "started_at": started,
            "finished_at": _now(),
            "rc": rc,
            "latest_plan": _latest_plan_summary(root, line),
            "latest_strategy_pipeline_report": _read_json(root / "DATA" / "reports" / "latest_strategy_pipeline_report.json"),
        }
    return results


def _report(root: Path, payload: dict[str, Any]) -> Path:
    path = root / "docs" / "reports" / f"STEP7.134_strategy5_6_v5_gate_paper_simulation_{_stamp()}.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    after = payload.get("paper_after") or {}
    orders = after.get("paper_orders_by_gate") or []
    skips = after.get("paper_skip_ledger_by_gate") or []
    pass_count = sum(int(row.get("n") or 0) for row in orders if row.get("gate_decision") == "pass")
    blocked_count = sum(int(row.get("n") or 0) for row in skips if row.get("gate_decision") == "blocked")
    missing_count = sum(int(row.get("n") or 0) for row in skips if row.get("gate_decision") == "feature_missing")
    lines = [
        "# STEP7.134 Strategy5/6 V5 Gate Paper Simulation",
        "",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- status: `{payload.get('status')}`",
        f"- experiment_id: `{payload.get('experiment_id')}`",
        f"- paper_epoch_id: `{payload.get('paper_epoch_id')}`",
        f"- gate_config: `{payload.get('gate_config', {}).get('path')}`",
        f"- gate_config_backup: `{payload.get('gate_config', {}).get('backup_path')}`",
        "",
        "## Preflight",
        "",
        f"- gate_validation_errors: `{payload.get('gate_config', {}).get('validation_errors')}`",
        f"- paper_db: `{(payload.get('paper_before') or {}).get('db_path')}`",
        f"- paper_db_exists: `{(payload.get('paper_before') or {}).get('exists')}`",
        f"- open_positions_before: `{(payload.get('paper_before') or {}).get('open_positions')}`",
        "",
        "## Pipeline Smoke",
        "",
        "| line | rc | plan status | run_id | executable | paper eligible |",
        "| --- | ---: | --- | --- | ---: | ---: |",
    ]
    for line, item in (payload.get("smoke") or {}).items():
        plan = item.get("latest_plan") or {}
        lines.append(
            f"| `{line}` | {item.get('rc')} | `{plan.get('status')}` | `{plan.get('run_id')}` | "
            f"{plan.get('executable_count')} | {plan.get('paper_eligible_count')} |"
        )
    lines.extend(
        [
            "",
            "## Gate Ledger",
            "",
            f"- paper_orders gate pass: `{pass_count}`",
            f"- paper_skip_ledger blocked: `{blocked_count}`",
            f"- paper_skip_ledger feature_missing: `{missing_count}`",
            "",
            "### Orders By Gate",
            "",
            "```json",
            json.dumps(orders, ensure_ascii=False, indent=2),
            "```",
            "",
            "### Skips By Gate",
            "",
            "```json",
            json.dumps(skips, ensure_ascii=False, indent=2),
            "```",
            "",
            "## Judgment",
            "",
        ]
    )
    if payload.get("status") == "ok":
        lines.append("- Smoke paper simulation started and produced gate lineage in the paper ledger.")
    else:
        lines.append("- Smoke did not satisfy all DoD checks. Do not advance to STEP7.135 until the blocking reasons are resolved.")
    lines.append("- This is still bounded paper validation, not production promotion.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run(root: Path, *, smoke: bool = True) -> dict[str, Any]:
    stamp = _stamp()
    generated_at = _now()
    gate = _prepare_gate_config(root, stamp=stamp)
    experiment_id = gate["experiment_id"]
    payload: dict[str, Any] = {
        "task_id": TASK_ID,
        "schema_version": "step7.134-paper-simulation-v1",
        "generated_at": generated_at,
        "experiment_id": experiment_id,
        "paper_epoch_id": gate["paper_epoch_id"],
        "default_config": _default_config_snapshot(root),
        "gate_config": {k: v for k, v in gate.items() if k != "config"},
        "paper_daemon_status_before": read_paper_status(root, load_paper_config(root)),
        "paper_before": _paper_counts(root, experiment_id=experiment_id),
    }
    if gate["validation_errors"]:
        payload["status"] = "blocked_preflight"
        payload["reason_codes"] = gate["validation_errors"]
    else:
        payload["smoke"] = _run_smoke(root) if smoke else {}
        payload["paper_after"] = _paper_counts(root, experiment_id=experiment_id)
        payload["paper_daemon_status_after"] = read_paper_status(root, load_paper_config(root))
        orders = (payload.get("paper_after") or {}).get("paper_orders_by_gate") or []
        skips = (payload.get("paper_after") or {}).get("paper_skip_ledger_by_gate") or []
        gate_rows = len(orders) + len(skips)
        line_rcs = [int(row.get("rc") or 0) for row in (payload.get("smoke") or {}).values()]
        if any(rc != 0 for rc in line_rcs):
            payload["status"] = "partial_pipeline_error"
            payload["reason_codes"] = ["strategy_pipeline_rc_nonzero"]
        elif gate_rows <= 0:
            payload["status"] = "insufficient_gate_lineage"
            payload["reason_codes"] = ["no_new_paper_gate_rows_for_experiment"]
        else:
            payload["status"] = "ok"
            payload["reason_codes"] = []
    report = _report(root, payload)
    payload["report"] = str(report)
    output = root / OUTPUT_JSON_REL
    _write_json(output, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--no-smoke", action="store_true")
    args = parser.parse_args()
    payload = run(Path(args.project_root).resolve(), smoke=not args.no_smoke)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
