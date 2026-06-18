from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "DATA" / "research" / "trade_snapshots"
REPORT_PATH = ROOT / "docs" / "reports" / "STEP29.1_existing_db_field_inventory_snapshot_gap_audit_20260617.md"
JSON_PATH = OUT_DIR / "step29_1_existing_db_field_inventory_snapshot_gap_audit.json"

MARKET_HINTS = (
    "rsi",
    "ema",
    "boll",
    "atr",
    "oi",
    "open_interest",
    "funding",
    "volume_z",
    "btc",
    "ret",
    "return",
    "ohlcv",
    "candle",
)
DIRECT_MARKET_SNAPSHOT_HINTS = (
    "rsi",
    "ema",
    "boll",
    "atr",
    "oi",
    "open_interest",
    "funding",
    "volume_z",
    "btc",
)
TQ_HINTS = (
    "net_r",
    "mfe",
    "mae",
    "holding",
    "exit_reason",
    "root_cause",
    "quality",
    "label",
)
CONFIG_GATE_HINTS = (
    "config",
    "gate",
    "parameter",
    "param",
    "source_json",
    "payload",
    "rule",
    "hash",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ro_uri(path: Path) -> str:
    return f"file:{path.resolve().as_posix()}?mode=ro"


def connect_ro(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(ro_uri(path), uri=True)
    con.row_factory = sqlite3.Row
    return con


def list_source_dbs() -> list[Path]:
    explicit = [
        ROOT / "DATA" / "paper" / "paper_trading.db",
        ROOT / "DATA" / "backtest" / "p21_parameter_optimization.db",
        ROOT / "DATA" / "backtest" / "p21_backtest.db",
        ROOT / "DATA" / "audit" / "run_audit.db",
        ROOT / "DATA" / "strategy4" / "strategy4_observe.db",
        ROOT / "DATA" / "strategy5" / "strategy5.db",
        ROOT / "DATA" / "strategy6" / "strategy6.db",
        ROOT / "DATA" / "micro" / "micro_training.db",
        ROOT / "DATA" / "sandboxes" / "sandbox_registry.db",
    ]
    archives = sorted(
        (ROOT / "DATA" / "paper" / "archives").glob("*/paper_trading.db"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )[:40]
    paper_equivalent = sorted(
        (ROOT / "DATA" / "backtest" / "paper_equivalent").glob("*/paper_equivalent.db"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )[:20]
    sandboxes = sorted(
        (ROOT / "DATA" / "sandboxes").glob("**/sandbox.db"),
        key=lambda p: p.stat().st_mtime if p.exists() else 0,
        reverse=True,
    )[:30]
    seen: set[Path] = set()
    result: list[Path] = []
    for path in [*explicit, *archives, *paper_equivalent, *sandboxes]:
        if path.exists() and path not in seen:
            seen.add(path)
            result.append(path)
    return result


def table_names(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [row["name"] for row in rows]


def table_columns(con: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = con.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
    return [
        {
            "name": row["name"],
            "type": row["type"],
            "notnull": bool(row["notnull"]),
            "pk": bool(row["pk"]),
        }
        for row in rows
    ]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def safe_count(con: sqlite3.Connection, table: str) -> int | None:
    try:
        return int(con.execute(f"SELECT COUNT(*) AS c FROM {quote_ident(table)}").fetchone()["c"])
    except sqlite3.Error:
        return None


def read_only_check(path: Path) -> dict[str, Any]:
    try:
        con = connect_ro(path)
        try:
            con.execute("CREATE TABLE __step29_ro_probe(x INTEGER)")
            return {"ok": False, "error": "unexpected_write_succeeded"}
        except sqlite3.OperationalError as exc:
            return {"ok": True, "error": str(exc)}
        finally:
            con.close()
    except sqlite3.Error as exc:
        return {"ok": False, "error": str(exc)}


def classify_db(path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    if "/paper/archives/" in rel:
        return "paper_archive"
    if rel == "DATA/paper/paper_trading.db":
        return "paper_current"
    if "/paper_equivalent/" in rel:
        return "paper_equivalent_backtest"
    if "/sandboxes/" in rel:
        return "sandbox"
    if "/backtest/" in rel:
        return "backtest"
    if "/strategy" in rel:
        return "strategy_db"
    if "/micro/" in rel:
        return "micro"
    if "/audit/" in rel:
        return "audit"
    return "other"


def inventory_db(path: Path) -> dict[str, Any]:
    rel = path.relative_to(ROOT).as_posix()
    db: dict[str, Any] = {
        "path": rel,
        "kind": classify_db(path),
        "size_bytes": path.stat().st_size,
        "read_only_probe": read_only_check(path),
        "tables": [],
        "error": None,
    }
    try:
        con = connect_ro(path)
        try:
            for table in table_names(con):
                columns = table_columns(con, table)
                db["tables"].append(
                    {
                        "name": table,
                        "row_count": safe_count(con, table),
                        "columns": columns,
                    }
                )
        finally:
            con.close()
    except sqlite3.Error as exc:
        db["error"] = str(exc)
    return db


def collect_field_hits(inventories: list[dict[str, Any]], hints: tuple[str, ...]) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for db in inventories:
        for table in db.get("tables", []):
            for col in table.get("columns", []):
                name = col["name"].lower()
                if any(hint in name for hint in hints):
                    hits.append(
                        {
                            "db": db["path"],
                            "kind": db["kind"],
                            "table": table["name"],
                            "column": col["name"],
                            "row_count": table["row_count"],
                        }
                    )
    return hits


def fetch_rows(con: sqlite3.Connection, query: str, params: tuple[Any, ...] = (), limit: int | None = None) -> list[dict[str, Any]]:
    if limit is not None:
        query = f"{query} LIMIT {int(limit)}"
    return [dict(row) for row in con.execute(query, params).fetchall()]


def find_sample_paper_order(dbs: list[Path]) -> dict[str, Any] | None:
    candidates = [p for p in dbs if classify_db(p) == "paper_archive"] + [
        p for p in dbs if classify_db(p) == "paper_current"
    ]
    for path in candidates:
        try:
            con = connect_ro(path)
            try:
                names = set(table_names(con))
                if "paper_fills" not in names or "paper_orders" not in names:
                    continue
                cols = [c["name"] for c in table_columns(con, "paper_fills")]
                if "order_id" not in cols:
                    continue
                rows = con.execute(
                    """
                    SELECT order_id, COUNT(*) AS fill_count
                    FROM paper_fills
                    WHERE order_id IS NOT NULL
                    GROUP BY order_id
                    HAVING COUNT(*) >= 2
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                    """
                ).fetchall()
                if not rows:
                    continue
                order_id = rows[0]["order_id"]
                fills = fetch_rows(
                    con,
                    "SELECT * FROM paper_fills WHERE order_id = ? ORDER BY rowid",
                    (order_id,),
                )
                order_cols = [c["name"] for c in table_columns(con, "paper_orders")]
                if "order_id" in order_cols:
                    orders = fetch_rows(
                        con,
                        "SELECT * FROM paper_orders WHERE id = ? OR order_id = ?",
                        (order_id, order_id),
                        limit=5,
                    )
                else:
                    orders = fetch_rows(
                        con,
                        "SELECT * FROM paper_orders WHERE id = ?",
                        (order_id,),
                        limit=5,
                    )
                positions = []
                if "paper_positions" in names:
                    positions = fetch_rows(
                        con,
                        "SELECT * FROM paper_positions WHERE order_id = ? ORDER BY rowid",
                        (order_id,),
                    )
                tq = []
                for tq_table in ("trade_quality_samples", "paper_trade_quality_samples"):
                    if tq_table in names:
                        tq_cols = [c["name"] for c in table_columns(con, tq_table)]
                        if "order_id" in tq_cols:
                            tq = fetch_rows(
                                con,
                                f"SELECT * FROM {quote_ident(tq_table)} WHERE order_id = ? ORDER BY rowid",
                                (order_id,),
                                limit=5,
                            )
                            if tq:
                                break
                return {
                    "source_db": path.relative_to(ROOT).as_posix(),
                    "order_id": order_id,
                    "fill_count": int(rows[0]["fill_count"]),
                    "fill_columns": cols,
                    "fill_rows": fills,
                    "order_rows": orders,
                    "position_rows": positions,
                    "trade_quality_rows": tq,
                }
            finally:
                con.close()
        except sqlite3.Error:
            continue
    return None


def summarize_table_presence(inventories: list[dict[str, Any]]) -> dict[str, Any]:
    counter: Counter[str] = Counter()
    by_kind: dict[str, Counter[str]] = defaultdict(Counter)
    for db in inventories:
        for table in db.get("tables", []):
            counter[table["name"]] += 1
            by_kind[db["kind"]][table["name"]] += 1
    return {
        "top_tables": counter.most_common(50),
        "by_kind": {kind: ctr.most_common(30) for kind, ctr in sorted(by_kind.items())},
    }


def classify_gaps(sample: dict[str, Any] | None, market_hits: list[dict[str, Any]], tq_hits: list[dict[str, Any]], config_hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    if not sample:
        gaps.append(
            {
                "priority": "P0",
                "area": "paper_order_pairing",
                "gap": "No paper archive order with at least two fills was found in scanned DBs.",
                "recommendation": "Run a paper archive smoke or extend scan range before materializer implementation.",
            }
        )
    else:
        fill_cols = {c.lower() for c in sample["fill_columns"]}
        direct_market = [c for c in fill_cols if any(h in c for h in DIRECT_MARKET_SNAPSHOT_HINTS)]
        direct_tq = [c for c in fill_cols if any(h in c for h in TQ_HINTS)]
        if not direct_market:
            gaps.append(
                {
                    "priority": "P0",
                    "area": "paper_fill_market_snapshot",
                    "gap": "paper_fills does not directly carry RSI/EMA/Boll/ATR/OI/funding style market snapshot fields.",
                    "recommendation": "Reconstruct entry/exit market snapshots in sidecar using symbol + candle/event time and known-at policy.",
                }
            )
        if not direct_tq:
            gaps.append(
                {
                    "priority": "P0",
                    "area": "paper_fill_trade_quality",
                    "gap": "paper_fills does not directly carry Trade Quality labels such as net_R/MFE_R/MAE_R/holding_time.",
                    "recommendation": "Join or derive Trade Quality into outcome/label JSON; keep it out of decision-time input.",
                }
            )
    if not market_hits:
        gaps.append(
            {
                "priority": "P1",
                "area": "market_feature_sources",
                "gap": "No market feature columns were discovered in scanned DBs.",
                "recommendation": "Add a feature reconstruction source before dataset export.",
            }
        )
    if not tq_hits:
        gaps.append(
            {
                "priority": "P1",
                "area": "trade_quality_sources",
                "gap": "No Trade Quality columns were discovered in scanned DBs.",
                "recommendation": "Generate or locate TQ samples before LLM dataset export.",
            }
        )
    if not config_hits:
        gaps.append(
            {
                "priority": "P2",
                "area": "config_gate_lineage",
                "gap": "No config/gate lineage candidate columns were discovered.",
                "recommendation": "Persist config/gate source refs in sidecar from source_json and config files.",
            }
        )
    gaps.append(
        {
            "priority": "P0",
            "area": "source_db_boundary",
            "gap": "P29 must not fill these gaps by modifying source DB schemas or writing back to source tables.",
            "recommendation": "Use read-only extractors and write missing/backfill status only in DATA/research/trade_snapshots/trade_snapshots.db.",
        }
    )
    return gaps


def make_report(data: dict[str, Any]) -> str:
    db_count = len(data["inventories"])
    table_count = sum(len(db.get("tables", [])) for db in data["inventories"])
    sample = data.get("sample_paper_order")
    lines: list[str] = [
        "# STEP29.1 Existing DB Field Inventory And Snapshot Gap Audit",
        "",
        "> 状态：DONE",
        "> 日期：2026-06-17",
        f"> JSON：`{JSON_PATH.relative_to(ROOT).as_posix()}`",
        "",
        "## 结论",
        "",
        f"本次只读扫描了 {db_count} 个 SQLite DB、{table_count} 个表。P29 训练 DB 不能直接复用现有业务 DB schema：现有 paper fill 能记录订单执行事实，但 entry/exit 时点的市场特征、Trade Quality、config/gate lineage 分散在不同表或不同 DB，不能视为完整训练 snap。",
        "",
        "关键结论：",
        "",
        "- `paper_fills` 可以作为 entry/exit 执行事实来源，但不是完整 market/TQ snapshot。",
        "- entry 和 exit 可以通过 `order_id` 成对关联；训练样本应是一笔 order 一行、entry/exit event 两行。",
        "- RSI/EMA/Boll/ATR/OI/funding 等市场字段需要按 known-at policy 重建或从专门 feature 表只读抽取。",
        "- `MFE_R`、`MAE_R`、`net_R`、`holding_time`、`exit_reason` 等属于 outcome/label/post-trade review，不得进入 entry decision-time input。",
        "- 所有缺口都必须写入 sidecar 的 missing/needs_backfill，不允许修改 source DB。",
        "",
        "## Paper 成对订单样例",
        "",
    ]
    if sample:
        lines.extend(
            [
                f"- Source DB：`{sample['source_db']}`",
                f"- Order ID：`{sample['order_id']}`",
                f"- Fill rows：{sample['fill_count']}",
                f"- Fill columns：`{', '.join(sample['fill_columns'])}`",
                f"- Order rows：{len(sample['order_rows'])}",
                f"- Position rows：{len(sample['position_rows'])}",
                f"- Trade Quality rows：{len(sample['trade_quality_rows'])}",
                "",
            ]
        )
    else:
        lines.extend(["未在扫描范围内找到至少 2 条 fill 的 paper order。", ""])
    lines.extend(
        [
            "## 字段来源摘要",
            "",
            f"- Market hint columns：{len(data['market_field_hits'])}",
            f"- Trade Quality hint columns：{len(data['trade_quality_field_hits'])}",
            f"- Config/Gate hint columns：{len(data['config_gate_field_hits'])}",
            "",
            "## 优先缺口",
            "",
        ]
    )
    for gap in data["gaps"]:
        lines.append(f"- {gap['priority']} `{gap['area']}`：{gap['gap']} 建议：{gap['recommendation']}")
    lines.extend(
        [
            "",
            "## DB / Table Inventory 摘要",
            "",
        ]
    )
    for kind, tables in data["table_presence"]["by_kind"].items():
        rendered = ", ".join(f"{name}({count})" for name, count in tables[:12])
        lines.append(f"- `{kind}`：{rendered}")
    lines.extend(
        [
            "",
            "## Read-Only 边界验证",
            "",
            "脚本对每个可打开 DB 使用 SQLite URI `mode=ro`，并尝试创建 probe table；预期结果是写入失败。结果已记录在 JSON 的 `read_only_probe` 字段。",
            "",
            "## 对后续任务的要求",
            "",
            "- STEP29.2 schema 必须把 `decision_time_input_json` 与 outcome/label 分开。",
            "- STEP29.3 必须补 market feature known-at/available-time 规则。",
            "- STEP29.4/29.5 物化器只能写 sidecar DB。",
            "- STEP29.7 导出时必须做 leakage scan 和 coverage audit。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dbs = list_source_dbs()
    inventories = [inventory_db(path) for path in dbs]
    market_hits = collect_field_hits(inventories, MARKET_HINTS)
    tq_hits = collect_field_hits(inventories, TQ_HINTS)
    config_hits = collect_field_hits(inventories, CONFIG_GATE_HINTS)
    sample = find_sample_paper_order(dbs)
    data = {
        "step": "STEP29.1",
        "status": "done",
        "generated_at": now_iso(),
        "policy": {
            "source_db_access": "read_only_uri_mode_ro",
            "source_db_write_back_allowed": False,
            "output_scope": "DATA/research/trade_snapshots and docs/reports only",
        },
        "inventories": inventories,
        "table_presence": summarize_table_presence(inventories),
        "market_field_hits": market_hits[:500],
        "trade_quality_field_hits": tq_hits[:500],
        "config_gate_field_hits": config_hits[:500],
        "sample_paper_order": sample,
        "gaps": classify_gaps(sample, market_hits, tq_hits, config_hits),
    }
    JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    REPORT_PATH.write_text(make_report(data), encoding="utf-8")
    print(json.dumps({"json": str(JSON_PATH), "report": str(REPORT_PATH), "dbs": len(dbs)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
