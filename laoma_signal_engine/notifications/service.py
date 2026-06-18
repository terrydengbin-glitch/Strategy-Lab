from __future__ import annotations

from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now
from laoma_signal_engine.notifications.card import build_trade_plan_card, card_summary
from laoma_signal_engine.notifications.config import FeishuConfig, load_feishu_config
from laoma_signal_engine.notifications.delivery import append_delivery, dedup_key, delivery_row, has_delivery, read_delivery_history
from laoma_signal_engine.notifications.feishu_client import send_interactive_card
from laoma_signal_engine.notifications.selector import load_trade_plan_docs, mock_trade_plan_docs, select_trade_plan_signals

LATEST_DELIVERY_REPORT_PATH = Path("DATA/notifications/latest_delivery_report.json")


def send_trade_plan_notifications(
    project_root: Path,
    *,
    mock_signals: bool = False,
    mock_send: bool = False,
    force_enabled: bool = False,
    config: FeishuConfig | None = None,
    line: str | None = None,
) -> dict[str, Any]:
    root = project_root.resolve()
    cfg = config or load_feishu_config(root)
    docs = mock_trade_plan_docs() if mock_signals else load_trade_plan_docs(root, line=line)
    paper_summary = _read_paper_summary(root)
    selection = select_trade_plan_signals(docs, config=cfg, paper_summary=paper_summary, line=line)
    deliveries: list[dict[str, Any]] = []
    status = "ok"
    skip_reason = None
    if not mock_send and not force_enabled and not cfg.enabled:
        status = "skipped"
        skip_reason = "feishu_disabled"
        deliveries = _append_disabled_delivery_rows(root, selection, cfg=cfg, line=line)
        return _write_latest_report(
            root,
            {
                "selected": selection["selected_counts"],
                "selected_items": selection["selected"],
                "skipped": selection["skipped"],
                "deliveries": deliveries,
                "delivery_counts": _delivery_counts(deliveries),
                "mock_signals": mock_signals,
                "mock_send": mock_send,
                "line": line,
                "status": status,
                "skip_reason": skip_reason,
                "feishu": cfg.public_dict(),
            },
        )

    for signal in selection["selected"]:
        key = dedup_key(signal)
        if has_delivery(root, key):
            row = delivery_row(signal, key, {"sent": False, "error": None}, status="duplicate")
            append_delivery(root, row)
            deliveries.append(row)
            continue
        card = build_trade_plan_card(signal)
        result = send_interactive_card(card, cfg, mock=mock_send, force_enabled=force_enabled)
        delivery_status = _delivery_status(result)
        row = delivery_row(signal, key, result, status=delivery_status)
        row["card_summary"] = card_summary(card)
        append_delivery(root, row)
        deliveries.append(row)
    status_counts = _delivery_counts(deliveries)
    if status_counts["failed"] and (
        status_counts["success"] or status_counts["mock_sent"] or status_counts["duplicate"]
    ):
        status = "partial"
    elif status_counts["failed"]:
        status = "failed"
    elif status_counts["success"] or status_counts["mock_sent"]:
        status = "sent"
    elif status_counts["duplicate"]:
        status = "duplicate"
    return _write_latest_report(root, {
        "selected": selection["selected_counts"],
        "selected_items": selection["selected"],
        "skipped": selection["skipped"],
        "deliveries": deliveries,
        "delivery_counts": status_counts,
        "mock_signals": mock_signals,
        "mock_send": mock_send,
        "line": line,
        "status": status,
        "skip_reason": skip_reason,
        "feishu": cfg.public_dict(),
    })


def _append_disabled_delivery_rows(
    root: Path,
    selection: dict[str, Any],
    *,
    cfg: FeishuConfig,
    line: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    selected = [s for s in selection.get("selected") or [] if isinstance(s, dict)]
    skipped = [s for s in selection.get("skipped") or [] if isinstance(s, dict)]
    lines = (line,) if line else tuple(cfg.notify_lines)
    for strategy_line in lines:
        line_selected = [s for s in selected if s.get("strategy_line") == strategy_line]
        line_skipped = [s for s in skipped if s.get("strategy_line") == strategy_line]
        target_set_id = next((str(s.get("target_set_id") or "") for s in line_selected if s.get("target_set_id")), "")
        run_id = next((str(s.get("run_id") or "") for s in line_selected if s.get("run_id")), "")
        cycle_id = next((str(s.get("cycle_id") or "") for s in line_selected if s.get("cycle_id")), "")
        row = {
            "dedup_key": f"feishu:disabled:{strategy_line}:{run_id}:{cycle_id}:{target_set_id}",
            "channel": "feishu",
            "event_type": "trade_plan_executable",
            "strategy_line": strategy_line,
            "strategy_name": cfg.strategy_name(strategy_line),
            "run_id": run_id,
            "cycle_id": cycle_id,
            "target_set_id": target_set_id,
            "selected_count": len(line_selected),
            "skipped_count": len(line_skipped),
            "status": "skipped",
            "skip_reason": "feishu_disabled",
            "sent": False,
            "created_at": to_iso_z(utc_now()),
        }
        append_delivery(root, row)
        rows.append(row)
    return rows


def delivery_history(project_root: Path) -> list[dict[str, Any]]:
    payload = read_delivery_history(project_root.resolve())
    return list(payload.get("deliveries") or [])


def _delivery_status(result: dict[str, Any]) -> str:
    if result.get("status") == "mock_sent":
        return "mock_sent"
    if result.get("ok") and result.get("sent"):
        return "success"
    if result.get("ok") and not result.get("sent"):
        return "disabled"
    return "failed"


def _delivery_counts(deliveries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "success": 0,
        "mock_sent": 0,
        "failed": 0,
        "duplicate": 0,
        "disabled": 0,
        "skipped": 0,
        "other": 0,
    }
    for row in deliveries:
        status = str(row.get("status") or "")
        if status in counts:
            counts[status] += 1
        else:
            counts["other"] += 1
    return counts


def _read_paper_summary(project_root: Path) -> dict[str, Any] | None:
    path = project_root / "DATA/paper/latest_paper_state.json"
    if not path.exists():
        return None
    try:
        got = read_json_object(path)
    except (OSError, ValueError):
        return None
    return got if isinstance(got, dict) else None


def _write_latest_report(project_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    generated_at = to_iso_z(utc_now())
    report = _merge_latest_summary(project_root, payload, generated_at=generated_at)
    write_json_atomic(project_root / LATEST_DELIVERY_REPORT_PATH, report)
    return report


def _merge_latest_summary(project_root: Path, payload: dict[str, Any], *, generated_at: str) -> dict[str, Any]:
    run_id, cycle_id = _payload_run_cycle(payload)
    line = payload.get("line")
    lines = (line,) if line else tuple((payload.get("selected") or {}).keys() or ("without_micro", "micro_fast", "micro_full", "strategy4"))
    line_updates = {
        item: _line_delivery_summary(item, payload, generated_at=generated_at)
        for item in lines
        if item in {"without_micro", "micro_fast", "micro_full", "strategy4"}
    }
    existing = _read_latest_summary(project_root, run_id=run_id, cycle_id=cycle_id)
    merged_lines = dict(existing.get("lines") or {})
    merged_lines.update(line_updates)
    counts = _sum_line_counts(merged_lines)
    summary_status = _summary_status(merged_lines, counts)
    selected_items = list(existing.get("selected_items") or [])
    selected_items.extend(payload.get("selected_items") or [])
    deliveries = list(existing.get("deliveries") or [])
    deliveries.extend(payload.get("deliveries") or [])
    return {
        "schema_version": "15.16",
        "source": "runtime_executable_feishu_delivery_summary",
        "generated_at": generated_at,
        "run_id": run_id,
        "cycle_id": cycle_id,
        "status": payload.get("status") or summary_status,
        "summary_status": summary_status,
        "lines": merged_lines,
        "delivery_counts": counts,
        "selected": payload.get("selected") or {},
        "selected_items": selected_items,
        "skipped": payload.get("skipped") or [],
        "deliveries": deliveries,
        "mock_signals": bool(payload.get("mock_signals")),
        "mock_send": bool(payload.get("mock_send")),
        "skip_reason": payload.get("skip_reason"),
        "feishu": payload.get("feishu") or {},
    }


def _read_latest_summary(project_root: Path, *, run_id: str | None, cycle_id: str | None) -> dict[str, Any]:
    path = project_root / LATEST_DELIVERY_REPORT_PATH
    if not path.is_file():
        return {}
    try:
        got = read_json_object(path)
    except (OSError, ValueError, TypeError):
        return {}
    if not isinstance(got, dict):
        return {}
    if got.get("schema_version") != "15.16":
        return {}
    if got.get("run_id") != run_id or got.get("cycle_id") != cycle_id:
        return {}
    return got


def _payload_run_cycle(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    rows = list(payload.get("deliveries") or []) + list(payload.get("selected_items") or [])
    for row in rows:
        if isinstance(row, dict) and (row.get("run_id") or row.get("cycle_id")):
            return row.get("run_id"), row.get("cycle_id")
    return None, None


def _line_delivery_summary(line: str, payload: dict[str, Any], *, generated_at: str) -> dict[str, Any]:
    selected = [s for s in payload.get("selected_items") or [] if isinstance(s, dict) and s.get("strategy_line") == line]
    skipped = [s for s in payload.get("skipped") or [] if isinstance(s, dict) and s.get("strategy_line") == line]
    deliveries = [d for d in payload.get("deliveries") or [] if isinstance(d, dict) and d.get("strategy_line") == line]
    counts = _delivery_counts(deliveries)
    status = _summary_status({line: {"delivery_counts": counts, "selected_count": len(selected)}}, counts)
    skip_reason = next((d.get("skip_reason") for d in deliveries if d.get("skip_reason")), payload.get("skip_reason"))
    return {
        "status": status,
        "selected_count": len(selected) if selected else int((payload.get("selected") or {}).get(line) or 0),
        "sent_count": counts["success"] + counts["mock_sent"] + counts["duplicate"],
        "failed_count": counts["failed"],
        "skipped_count": counts["skipped"] + len(skipped),
        "skip_reason": skip_reason,
        "delivery_counts": counts,
        "latest_delivery_at": generated_at,
    }


def _sum_line_counts(lines: dict[str, Any]) -> dict[str, int]:
    total = {
        "success": 0,
        "mock_sent": 0,
        "failed": 0,
        "duplicate": 0,
        "disabled": 0,
        "skipped": 0,
        "other": 0,
    }
    for row in lines.values():
        counts = row.get("delivery_counts") if isinstance(row, dict) else {}
        if not isinstance(counts, dict):
            continue
        for key in total:
            total[key] += int(counts.get(key) or 0)
    return total


def _summary_status(lines: dict[str, Any], counts: dict[str, int]) -> str:
    if counts.get("failed"):
        return "partial_failed" if any(v for k, v in counts.items() if k not in {"failed", "other"}) else "failed"
    if counts.get("success") or counts.get("mock_sent") or counts.get("duplicate"):
        return "success"
    if counts.get("skipped") or counts.get("disabled"):
        return "skipped"
    if lines and all(int((row or {}).get("selected_count") or 0) == 0 for row in lines.values()):
        return "no_signal"
    return "ok"
