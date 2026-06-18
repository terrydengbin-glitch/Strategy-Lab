from __future__ import annotations

from typing import Any


def build_trade_plan_card(signal: dict[str, Any]) -> dict[str, Any]:
    side = signal.get("side_text") or "-"
    title = f"【{signal.get('strategy_name') or signal.get('strategy_line') or '-'}】有效信号"
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "turquoise" if signal.get("side") == "LONG" else "red",
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {
                "tag": "div",
                "fields": [
                    _field("交易对", signal.get("symbol")),
                    _field("方向", side),
                    _field("进场价格", signal.get("entry_price")),
                    _field("SL", signal.get("stop_loss")),
                    _field("TP", signal.get("take_profit")),
                    _field("Risk", signal.get("risk_budget_usdt")),
                    _field("Notional", signal.get("notional_usdt")),
                    _field("Margin", signal.get("margin_usdt")),
                    _field("Leverage", signal.get("leverage")),
                ],
            },
            {"tag": "hr"},
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": "**历史统计**\n总订单：{orders}\n胜率：{win_rate}".format(
                        orders=_format_value(signal.get("paper_total_orders")),
                        win_rate=_format_win_rate(signal.get("paper_win_rate")),
                    ),
                },
            },
        ],
    }


def card_summary(card: dict[str, Any]) -> dict[str, Any]:
    header = card.get("header") or {}
    title = (header.get("title") or {}).get("content")
    texts: list[str] = []
    for element in card.get("elements") or []:
        for field in element.get("fields") or []:
            text = (field.get("text") or {}).get("content")
            if text:
                texts.append(str(text))
        text = (element.get("text") or {}).get("content")
        if text:
            texts.append(str(text))
    return {"title": title, "text": "\n".join(texts), "element_count": len(card.get("elements") or [])}


def _field(label: str, value: Any) -> dict[str, Any]:
    return {"is_short": True, "text": {"tag": "lark_md", "content": f"**{label}**\n{_format_value(value)}"}}


def _format_value(value: Any) -> str:
    if value in (None, ""):
        return "-"
    return f"{value:g}" if isinstance(value, float) else str(value)


def _format_win_rate(value: Any) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        text = str(value)
        return text if text.endswith("%") else f"{text}%"
