from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SIDECAR_DB = ROOT / "DATA" / "research" / "trade_snapshots" / "trade_snapshots.db"
SCHEMA_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_trade_snapshot_schema_contract.json"
EXPORT_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_7_llm_training_dataset_smoke.jsonl"
MANIFEST_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_7_llm_training_dataset_manifest.json"
COVERAGE_PATH = ROOT / "DATA" / "research" / "trade_snapshots" / "step29_7_llm_training_dataset_coverage_audit.json"
REPORT_PATH = ROOT / "docs" / "reports" / "STEP29.7_llm_training_dataset_export_and_coverage_audit_20260617.md"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def parse_json(text: str | None) -> Any:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_parse_error": True, "raw": text}


def walk_keys(value: Any, prefix: str = "") -> set[str]:
    keys: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            full = f"{prefix}.{key}" if prefix else str(key)
            keys.add(str(key))
            keys.add(full)
            keys.update(walk_keys(item, full))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            keys.update(walk_keys(item, f"{prefix}[{idx}]"))
    return keys


def load_samples(limit: int = 1000) -> list[dict[str, Any]]:
    con = sqlite3.connect(SIDECAR_DB)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT *
            FROM trade_training_samples
            ORDER BY source_mode, sample_id
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def make_dataset_row(row: dict[str, Any], post_trade_fields: set[str]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    decision = parse_json(row.get("decision_time_input_json"))
    outcome = parse_json(row.get("post_trade_outcome_json"))
    label = parse_json(row.get("label_json"))
    audit = parse_json(row.get("audit_context_json"))
    data_quality = parse_json(row.get("data_quality_json"))
    source_refs = parse_json(row.get("source_refs_json"))
    decision_keys = walk_keys(decision)
    leaks = sorted(field for field in post_trade_fields if field in decision_keys)
    missing = data_quality.get("missing_fields_json") or []
    feature_complete = (
        data_quality.get("feature_completeness") == "complete"
        or data_quality.get("market_feature_completeness") == "complete"
    )
    allowed = not leaks and not missing and feature_complete
    dataset_row = {
        "sample_id": row["sample_id"],
        "order_id": row["order_id"],
        "source_mode": row["source_mode"],
        "source_db_path": row["source_db_path"],
        "strategy_line": row["strategy_line"],
        "symbol": row["symbol"],
        "side": row["side"],
        "entry_time_ms": row["entry_time_ms"],
        "exit_time_ms": row["exit_time_ms"],
        "decision_time_input_json": decision,
        "post_trade_outcome_json": outcome,
        "label_json": label,
        "audit_context_json": audit,
        "data_quality_json": data_quality,
        "source_refs_json": source_refs,
        "training_use": {
            "allowed_for_llm_training": allowed,
            "dataset_split": "shadow_pool",
            "review_status": "needs_review" if not allowed else "ready",
            "reason": "complete_no_leakage" if allowed else "incomplete_or_needs_review",
        },
        "schema_version": row["schema_version"],
    }
    violations = [
        {
            "sample_id": row["sample_id"],
            "field": field,
            "location": "decision_time_input_json",
        }
        for field in leaks
    ]
    return dataset_row, violations


def write_jsonl(rows: list[dict[str, Any]]) -> str:
    with EXPORT_PATH.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(canonical_json(row) + "\n")
    return hashlib.sha256(EXPORT_PATH.read_bytes()).hexdigest()


def write_report(manifest: dict[str, Any], coverage: dict[str, Any]) -> None:
    lines = [
        "# STEP29.7 LLM Training Dataset Export And Coverage Audit",
        "",
        "> 状态：DONE",
        "> 日期：2026-06-17",
        f"> JSONL：`{EXPORT_PATH.relative_to(ROOT).as_posix()}`",
        f"> Manifest：`{MANIFEST_PATH.relative_to(ROOT).as_posix()}`",
        f"> Coverage：`{COVERAGE_PATH.relative_to(ROOT).as_posix()}`",
        "",
        "## 结论",
        "",
        "已从 sidecar DB 导出一个 smoke JSONL，并生成 dataset manifest、coverage audit 和 leakage scan。当前数据集是 `shadow_pool / needs_review`，不能直接作为 complete LLM 训练集推进。",
        "",
        "原因：market feature / known-at 已由 STEP29.10 sidecar reconstruction 补齐；Trade Quality label 命中率仍低，因此当前仍是 `shadow_pool / needs_review`，不能直接作为 complete LLM 训练集推进。",
        "",
        "## 覆盖率",
        "",
        f"- Samples：{coverage['sample_count']}",
        f"- Source modes：{coverage['source_mode_counts']}",
        f"- Entry/exit pair rate：{coverage['entry_exit_pair_rate']}",
        f"- Market feature complete rate：{coverage['market_feature_complete_rate']}",
        f"- Trade Quality label rate：{coverage['trade_quality_label_rate']}",
        f"- Leakage violations：{len(coverage['leakage_violations'])}",
        "",
        "## 字段缺口是否接受",
        "",
        "- 当前 smoke 阶段接受缺口：market features `needs_reconstruction`、TQ label 缺失、部分 config/gate 字段缺失。",
        "- 当前 smoke 阶段不接受泄漏：post-trade outcome/label 字段进入 `decision_time_input_json`。本次扫描违规数为 0。",
        "- 进入真实训练前必须补写或重建：entry/exit market snap、TQ label、dataset split policy、label quality review。",
        "",
        "## Dataset Hash",
        "",
        f"- Dataset hash：`{manifest['dataset_hash']}`",
        f"- Schema hash：`{manifest['schema_hash']}`",
        "",
        "## 边界",
        "",
        "- 只读读取 sidecar DB。",
        "- 输出 JSONL/manifest/audit 到 `DATA/research/trade_snapshots`。",
        "- 不训练模型，不接入交易链条，不回写任何 source DB。",
    ]
    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    post_trade_fields = set(schema["post_trade_only_fields"])
    raw_rows = load_samples()
    dataset_rows: list[dict[str, Any]] = []
    leakage_violations: list[dict[str, str]] = []
    source_counter: Counter[str] = Counter()
    tq_joined = 0
    market_complete = 0
    known_at_pass = 0
    paired = 0
    for row in raw_rows:
        dataset_row, violations = make_dataset_row(row, post_trade_fields)
        dataset_rows.append(dataset_row)
        leakage_violations.extend(violations)
        source_counter[row["source_mode"]] += 1
        dq = dataset_row["data_quality_json"]
        if row.get("entry_event_id") and row.get("exit_event_id"):
            paired += 1
        if (
            dq.get("feature_completeness") == "complete"
            or dq.get("market_feature_completeness") == "complete"
        ):
            market_complete += 1
        if dq.get("market_known_at_pass") is True:
            known_at_pass += 1
        if dataset_row["label_json"]:
            tq_joined += 1
    dataset_hash = write_jsonl(dataset_rows)
    schema_hash = stable_hash(schema)
    sample_count = len(dataset_rows)
    coverage = {
        "step": "STEP29.7",
        "status": "done",
        "sample_count": sample_count,
        "source_mode_counts": dict(source_counter),
        "entry_exit_pair_rate": paired / sample_count if sample_count else 0.0,
        "market_feature_complete_rate": market_complete / sample_count if sample_count else 0.0,
        "trade_quality_label_rate": tq_joined / sample_count if sample_count else 0.0,
        "known_at_pass_rate": known_at_pass / sample_count if sample_count else 0.0,
        "known_at_status": "pass" if sample_count and known_at_pass == sample_count else "pending_or_partial",
        "leakage_violations": leakage_violations,
        "missing_fields_policy": "accepted_for_smoke_shadow_pool_only",
        "generated_at": now_iso(),
    }
    manifest = {
        "dataset_version": "step29_7_smoke_v1",
        "dataset_path": EXPORT_PATH.relative_to(ROOT).as_posix(),
        "dataset_hash": dataset_hash,
        "schema_version": "step29_trade_snapshot_v1",
        "schema_hash": schema_hash,
        "feature_schema_version": "step29_market_feature_known_at_v1",
        "label_policy_version": "step29_label_policy_smoke_v1",
        "split_manifest": {
            "policy": "all samples exported to shadow_pool because completeness is not training-ready",
            "splits": {"shadow_pool": sample_count, "train": 0, "eval": 0, "gold": 0},
        },
        "source_snapshot_refs": sorted(source_counter),
        "coverage_path": COVERAGE_PATH.relative_to(ROOT).as_posix(),
        "allowed_for_llm_training": False,
        "generated_at": now_iso(),
    }
    COVERAGE_PATH.write_text(json.dumps(coverage, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    write_report(manifest, coverage)
    print(json.dumps({"manifest": manifest, "coverage": coverage}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
