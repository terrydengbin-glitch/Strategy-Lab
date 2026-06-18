"""V5 Trade Quality gate adapter for paper experiments.

The adapter is deliberately dormant unless DATA/paper/v5_trade_gate_experiment.json
exists and enables it.  This keeps the baseline paper chain unchanged outside
explicit audit experiments.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from laoma_signal_engine.paper.models import PaperIntent
from laoma_signal_engine.paper.utils import utc_now_iso


CONFIG_PATH = Path("DATA/paper/v5_trade_gate_experiment.json")
FACTOR_SNAPSHOT_PATHS = (
    Path("DATA/factors/latest_factor_snapshot.json"),
    Path("DATA/factors/latest_factor_snapshot_withoutoficvd.json"),
)


@dataclass(frozen=True)
class GateDecision:
    enabled: bool
    strategy_line: str
    decision: str
    action: str
    reason: str
    experiment_id: str | None = None
    paper_epoch_id: str | None = None
    parameter_set_id: str | None = None
    gate_candidate_id: str | None = None
    rule_json: dict[str, Any] | None = None
    features: dict[str, Any] | None = None
    missing_features: list[str] | None = None
    evaluated_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "strategy_line": self.strategy_line,
            "decision": self.decision,
            "action": self.action,
            "reason": self.reason,
            "experiment_id": self.experiment_id,
            "paper_epoch_id": self.paper_epoch_id,
            "parameter_set_id": self.parameter_set_id,
            "gate_candidate_id": self.gate_candidate_id,
            "rule_json": self.rule_json or {},
            "features": self.features or {},
            "missing_features": self.missing_features or [],
            "evaluated_at": self.evaluated_at,
            "schema_version": "paper_v5_trade_gate_v1",
        }


def load_gate_config(project_root: Path) -> dict[str, Any]:
    path = project_root / CONFIG_PATH
    if not path.exists():
        return {"enabled": False, "rules": {}}
    try:
        got = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"enabled": False, "rules": {}, "load_error": str(exc)}
    if not isinstance(got, dict):
        return {"enabled": False, "rules": {}, "load_error": "config_not_object"}
    got.setdefault("rules", {})
    return got


def evaluate_paper_v5_trade_gate(project_root: Path, intent: PaperIntent) -> GateDecision:
    config = load_gate_config(project_root)
    paper_epoch_id = _paper_epoch_id_for_line(config, intent.strategy_line)
    if not config.get("enabled"):
        return GateDecision(
            enabled=False,
            strategy_line=intent.strategy_line,
            decision="disabled",
            action="pass",
            reason="v5_trade_gate_disabled",
        )
    rules = config.get("rules") if isinstance(config.get("rules"), dict) else {}
    rule_cfg = rules.get(intent.strategy_line)
    if not isinstance(rule_cfg, dict):
        return GateDecision(
            enabled=True,
            strategy_line=intent.strategy_line,
            decision="no_rule",
            action="pass",
            reason="v5_trade_gate_no_rule_for_line",
            experiment_id=_str_or_none(config.get("experiment_id")),
            paper_epoch_id=paper_epoch_id,
            evaluated_at=utc_now_iso(),
        )
    rule_json = rule_cfg.get("rule_json") if isinstance(rule_cfg.get("rule_json"), dict) else {}
    features, missing = _collect_rule_features(project_root, intent, rule_json)
    if missing:
        action = str(config.get("feature_missing_policy") or rule_cfg.get("feature_missing_policy") or "block").lower()
        return GateDecision(
            enabled=True,
            strategy_line=intent.strategy_line,
            decision="feature_missing",
            action="block" if action == "block" else "pass",
            reason="v5_trade_gate_feature_missing",
            experiment_id=_str_or_none(config.get("experiment_id")),
            paper_epoch_id=paper_epoch_id,
            parameter_set_id=_str_or_none(rule_cfg.get("parameter_set_id")),
            gate_candidate_id=_str_or_none(rule_cfg.get("gate_candidate_id")),
            rule_json=rule_json,
            features=features,
            missing_features=missing,
            evaluated_at=utc_now_iso(),
        )
    matched = _rule_matches(rule_json, features)
    return GateDecision(
        enabled=True,
        strategy_line=intent.strategy_line,
        decision="blocked" if matched else "pass",
        action="block" if matched else "pass",
        reason="v5_trade_gate_blocked" if matched else "v5_trade_gate_pass",
        experiment_id=_str_or_none(config.get("experiment_id")),
        paper_epoch_id=paper_epoch_id,
        parameter_set_id=_str_or_none(rule_cfg.get("parameter_set_id")),
        gate_candidate_id=_str_or_none(rule_cfg.get("gate_candidate_id")),
        rule_json=rule_json,
        features=features,
        missing_features=[],
        evaluated_at=utc_now_iso(),
    )


def annotate_intent_with_gate(intent: PaperIntent, decision: GateDecision) -> None:
    payload = decision.as_dict()
    intent.guards["v5_trade_gate"] = payload
    intent.source_json["v5_trade_gate"] = payload


def _paper_epoch_id_for_line(config: dict[str, Any], strategy_line: str) -> str | None:
    line_epochs = config.get("line_epochs")
    if isinstance(line_epochs, dict):
        value = line_epochs.get(strategy_line)
        if value:
            return _str_or_none(value)
    return _str_or_none(config.get("paper_epoch_id"))


def _collect_rule_features(project_root: Path, intent: PaperIntent, rule_json: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    fields = _rule_fields(rule_json)
    features: dict[str, Any] = {}
    missing: list[str] = []
    roots = [intent.guards, intent.source_json]
    live_context = _live_factor_context(project_root, intent.symbol)
    for field in fields:
        value = _lookup_field(roots, field)
        if value is None:
            value = _derive_live_feature(live_context, intent, field)
        if value is None:
            missing.append(field)
        else:
            features[field] = value
    return features, missing


def _rule_fields(rule_json: dict[str, Any]) -> list[str]:
    got: list[str] = []
    if "field" in rule_json:
        got.append(str(rule_json["field"]))
    for item in rule_json.get("rules") or []:
        if isinstance(item, dict):
            got.extend(_rule_fields(item))
    return sorted(set(got))


def _rule_matches(rule_json: dict[str, Any], features: dict[str, Any]) -> bool:
    if "field" in rule_json:
        field = str(rule_json.get("field"))
        op = str(rule_json.get("op") or "eq").lower()
        expected = rule_json.get("value")
        value = features.get(field)
        if op in {"eq", "=="}:
            return _norm(value) == _norm(expected)
        if op in {"neq", "!="}:
            return _norm(value) != _norm(expected)
        if op in {"in"}:
            values = expected if isinstance(expected, list) else [expected]
            return _norm(value) in {_norm(item) for item in values}
        return False
    children = [item for item in (rule_json.get("rules") or []) if isinstance(item, dict)]
    operator = str(rule_json.get("operator") or "AND").upper()
    if not children:
        return False
    if operator == "OR":
        return any(_rule_matches(child, features) for child in children)
    return all(_rule_matches(child, features) for child in children)


def _lookup_field(roots: list[dict[str, Any]], field: str) -> Any:
    for root in roots:
        value = _recursive_lookup(root, field)
        if value is not None:
            return value
    return None


def _live_factor_context(project_root: Path, symbol: str) -> dict[str, Any]:
    wanted = str(symbol or "").upper()
    if not wanted:
        return {}
    for rel_path in FACTOR_SNAPSHOT_PATHS:
        path = project_root / rel_path
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        items = doc.get("items") if isinstance(doc, dict) else None
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and str(item.get("symbol") or "").upper() == wanted:
                return {
                    "snapshot_path": str(rel_path),
                    "snapshot_generated_at": doc.get("generated_at"),
                    **item,
                }
    return {}


def _derive_live_feature(context: dict[str, Any], intent: PaperIntent, field: str) -> Any:
    if not context:
        return None
    if field == "funding_bucket":
        funding = context.get("funding_context") if isinstance(context.get("funding_context"), dict) else {}
        return funding.get("funding_bucket") or _funding_bucket_from_rate(_float_or_none(funding.get("funding_rate_raw")))
    if field == "funding_crowded_side":
        funding = context.get("funding_context") if isinstance(context.get("funding_context"), dict) else {}
        return _funding_crowded_side(_float_or_none(funding.get("funding_rate_raw")))
    if field == "oi_state":
        oi = context.get("oi_15m") if isinstance(context.get("oi_15m"), dict) else {}
        return oi.get("oi_state")
    if field == "side_flow_alignment":
        flow_dir = _flow_direction(context)
        side_dir = _side_direction(intent.side)
        if flow_dir and side_dir:
            return "same" if flow_dir == side_dir else "opposite"
        return "neutral" if flow_dir is not None else None
    if field == "price_flow_alignment":
        price_dir = _price_direction(context)
        flow_dir = _flow_direction(context)
        if price_dir and flow_dir:
            return "same" if price_dir == flow_dir else "opposite"
        return "neutral" if price_dir is not None and flow_dir is not None else None
    return None


def _price_direction(context: dict[str, Any]) -> int | None:
    primary = context.get("primary_15m") if isinstance(context.get("primary_15m"), dict) else {}
    trigger = context.get("trigger_5m") if isinstance(context.get("trigger_5m"), dict) else {}
    for value in (primary.get("price_ret"), trigger.get("price_ret")):
        number = _float_or_none(value)
        if number is not None:
            if number > 0:
                return 1
            if number < 0:
                return -1
            return 0
    return None


def _flow_direction(context: dict[str, Any]) -> int | None:
    primary = context.get("primary_15m") if isinstance(context.get("primary_15m"), dict) else {}
    cvd_state = str(primary.get("kline_cvd_state") or "").lower()
    if cvd_state in {"buy_dominant", "strong_buy", "buy"}:
        return 1
    if cvd_state in {"sell_dominant", "strong_sell", "sell"}:
        return -1
    taker = _float_or_none(primary.get("taker_buy_ratio"))
    if taker is not None:
        if taker > 0.54:
            return 1
        if taker < 0.46:
            return -1
        return 0
    micro = context.get("micro_15m") if isinstance(context.get("micro_15m"), dict) else {}
    cvd = _float_or_none(micro.get("cvd"))
    if cvd is not None:
        if cvd > 0:
            return 1
        if cvd < 0:
            return -1
        return 0
    return None


def _side_direction(side: str) -> int | None:
    got = str(side or "").upper()
    if got == "LONG":
        return 1
    if got == "SHORT":
        return -1
    return None


def _funding_bucket_from_rate(rate: float | None) -> str | None:
    if rate is None:
        return None
    if rate >= 0.0005:
        return "OVERHEATED"
    if rate <= -0.0005:
        return "NEGATIVE_EXTREME"
    return "NEUTRAL"


def _funding_crowded_side(rate: float | None) -> str | None:
    if rate is None:
        return None
    if rate > 0:
        return "long"
    if rate < 0:
        return "short"
    return "neutral"


def _recursive_lookup(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        if field in value:
            return value.get(field)
        for item in value.values():
            got = _recursive_lookup(item, field)
            if got is not None:
                return got
    elif isinstance(value, list):
        for item in value:
            got = _recursive_lookup(item, field)
            if got is not None:
                return got
    return None


def _norm(value: Any) -> str:
    return str(value).strip().lower()


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
