"""Shared Binance payload helpers for micro adapters. docs/STEP3.1_任务卡.md section 5.5."""


def normalize_binance_symbol(raw: object) -> str:
    symbol = str(raw).strip().upper()
    if not symbol:
        msg = "empty symbol after strip/upper"
        raise ValueError(msg)
    return symbol
