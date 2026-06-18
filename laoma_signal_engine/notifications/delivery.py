from __future__ import annotations

from pathlib import Path
from typing import Any

from laoma_signal_engine.core.json_io import read_json_object, write_json_atomic
from laoma_signal_engine.core.time_utils import to_iso_z, utc_now


DELIVERY_PATH = Path("DATA/notifications/delivery_history.json")


def read_delivery_history(project_root: Path) -> dict[str, Any]:
    path = project_root / DELIVERY_PATH
    if not path.exists():
        return {"schema_version": "15.6", "generated_at": None, "deliveries": []}
    data = read_json_object(path)
    if isinstance(data, list):
        return {"schema_version": "15.6", "generated_at": None, "deliveries": data}
    if isinstance(data, dict):
        data.setdefault("deliveries", [])
        return data
    return {"schema_version": "15.6", "generated_at": None, "deliveries": []}


def dedup_key(signal: dict[str, Any]) -> str:
    return "feishu:trade_plan:{line}:{run}:{cycle}:{symbol}:{side}:{entry}:{sl}:{tp}".format(
        line=signal.get("strategy_line") or "",
        run=signal.get("run_id") or "",
        cycle=signal.get("cycle_id") or "",
        symbol=signal.get("symbol") or "",
        side=signal.get("side") or "",
        entry=signal.get("entry_price") or "",
        sl=signal.get("stop_loss") or "",
        tp=signal.get("take_profit") or "",
    )


def append_delivery(project_root: Path, row: dict[str, Any], *, max_items: int = 500) -> dict[str, Any]:
    payload = read_delivery_history(project_root)
    deliveries = list(payload.get("deliveries") or [])
    deliveries.append(row)
    payload["generated_at"] = to_iso_z(utc_now())
    payload["deliveries"] = deliveries[-max_items:]
    write_json_atomic(project_root / DELIVERY_PATH, payload)
    return payload


def has_delivery(project_root: Path, key: str) -> bool:
    payload = read_delivery_history(project_root)
    return any(row.get("dedup_key") == key and row.get("status") in {"success", "mock_sent"} for row in payload.get("deliveries") or [])


def delivery_row(signal: dict[str, Any], key: str, result: dict[str, Any], *, status: str) -> dict[str, Any]:
    return {
        "dedup_key": key,
        "channel": "feishu",
        "event_type": "trade_plan_executable",
        "strategy_line": signal.get("strategy_line"),
        "strategy_name": signal.get("strategy_name"),
        "run_id": signal.get("run_id"),
        "cycle_id": signal.get("cycle_id"),
        "symbol": signal.get("symbol"),
        "side": signal.get("side"),
        "status": status,
        "sent": bool(result.get("sent")),
        "error": result.get("error"),
        "status_code": result.get("status_code"),
        "response": result.get("response"),
        "created_at": to_iso_z(utc_now()),
    }
