# -*- coding: utf-8 -*-
"""
Funding 因子模块（Microstructure 2.0）

职责：
- 封装 Funding 因子的"纯计算逻辑"，不做任何 IO
- 输入：当前 funding_rate_raw、next_funding_time_ms、now_ts_ms、历史窗口
- 输出：Funding 因子族：
    * funding_rate_raw
    * hours_to_settlement
    * funding_bucket
    * funding_percentile
    * funding_z
    * funding_extreme_flag

参考文档：
- PRODUCT-FACTOR-FUNDING-V1.0.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence
import logging
import math
import statistics

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _clamp(value: float, lo: float, hi: float) -> float:
    """简单的 clamp 实现，防止 hours_to_settlement 越界。"""
    return max(lo, min(hi, value))


def _is_finite_number(x: float) -> bool:
    """判断是否为有限实数（排除 None / NaN / inf）。"""
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)


def _compute_empirical_percentile(
    history_values: Sequence[float],
    current_value: float,
) -> Optional[float]:
    """
    经验分布分位数估计 in [0,1]。

    实现说明（V1 版本，简单且稳健）：
    - 使用"排名百分比"：
        percentile = (# {v_i <= current_value}) / N
    - 不做复杂插值 / 权重，后续如有需要可升级为更精细的 quantile 实现
    """
    if not history_values:
        return None

    xs = sorted(v for v in history_values if _is_finite_number(v))
    n = len(xs)
    if n == 0:
        return None

    # 二分查找上界：找到<= current_value 的最后一个索引
    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        if xs[mid] <= current_value:
            lo = mid + 1
        else:
            hi = mid
    rank = lo  # 有多少个样本 <= current_value
    percentile = rank / n
    return _clamp(percentile, 0.0, 1.0)


def _compute_z_score(
    history_values: Sequence[float],
    current_value: float,
    std_floor: float,
) -> Optional[float]:
    """
    简单 z-score 计算：
        z = (x - μ) / max(σ, std_floor)

    注意：
    - 这里使用"样本标准差"或类似实现即可（statistics.pstdev / stdev 差异不敏感）
    - 若样本数不足，返回 None，由上层做缺失处理
    """
    xs = [float(v) for v in history_values if _is_finite_number(v)]
    n = len(xs)
    if n < 2:
        return None

    # P1-FIX: 确保 std_floor 是浮点数类型（防御性类型转换）
    # 如果配置文件中 std_floor 被解析为字符串（如 "1e-4"），需要转换
    try:
        std_floor_float = float(std_floor)
    except (ValueError, TypeError) as e:
        logger.warning(
            f"[Funding] std_floor 类型转换失败: {std_floor} (type={type(std_floor)}), "
            f"使用默认值 1e-4, 错误: {e}"
        )
        std_floor_float = 1e-4

    try:
        mean = statistics.fmean(xs)
        # 用总体标准差或样本标准差都可以，关键是要加 std_floor
        sigma = statistics.pstdev(xs)
    except Exception:
        logger.exception("[Funding] failed to compute mean/std from history")
        return None

    if sigma < std_floor_float:
        sigma = std_floor_float

    return (current_value - mean) / sigma


# ---------------------------------------------------------------------------
# Dataclasses for config & state
# ---------------------------------------------------------------------------

@dataclass
class FundingBucketsConfig:
    """
    Funding 档位配置（默认使用 PRODUCT-FACTOR-FUNDING-V1.0 中的 5 档）

    缺省值对应：
        NEG_EXTREME: <= -0.02
        NEG_HIGH:    (-0.02, -0.005]
        NEUTRAL:     (-0.005, 0.005)
        POS_HIGH:    [0.005, 0.02)
        POS_EXTREME: >= 0.02
    """
    neg_extreme: float = -0.02
    neg_high: float = -0.005
    pos_high: float = 0.005
    pos_extreme: float = 0.02


@dataclass
class FundingConfig:
    """
    Funding 全局配置（不含 IO，仅数学逻辑相关参数）。
    """
    # 分档配置：默认 + per-symbol 覆盖
    buckets_default: FundingBucketsConfig = field(
        default_factory=FundingBucketsConfig
    )
    buckets_per_symbol: Dict[str, FundingBucketsConfig] = field(
        default_factory=dict
    )

    # 极端分位数阈值（例如 0.95 表示上下 5% 为极端）
    extreme_percentile: float = 0.95

    # 历史窗口（以小时记，实际维护由上层完成）
    # 这里仅作为"文档 / 配置记录"，当前实现不直接用它截断数据
    percentile_window_hours: int = 24 * 90  # 约 90 天

    # Z-score 中 σ 的下限，防止几乎为 0 的情况
    std_floor: float = 1e-4

    # 样本最小阈值：低于这个数量不计算 percentile / z
    min_history_size: int = 50


@dataclass
class FundingHistoryState:
    """
    Funding 历史状态容器（按 symbol 独立维护）。

    使用方式建议：
    - 每个 symbol 拥有一个 FundingHistoryState 实例；
    - 调用 compute_funding_features() 时传入；
    - 默认会在函数末尾自动 append 当前样本（可用 update_history=False 关闭）。
    """
    values: List[float] = field(default_factory=list)
    max_len: int = 4000  # 约 N 笔，具体可在上层按实际需求设定

    def add(self, v: float) -> None:
        """
        追加一个样本并维持最大长度。
        
        注意：包含简单的去重逻辑，避免 Harvester 高频调用导致 history 被重复值填满。
        Funding Rate 通常 8 小时变一次（预测值变化频率稍高，但也非秒级），
        如果每分钟拉一次并 append，history 很快就会被大量重复值填满，
        导致 percentile 计算失效（变成了"最近 60 分钟分布"而不是"最近 90 天分布"）。
        
        建议：在 FeatureEngine 层控制，只有当 funding_rate 发生变化，
        或者每隔固定时间（如 1 小时）才调用 add()。
        """
        if not _is_finite_number(v):
            return
        
        # 去重逻辑：避免连续相同值重复添加
        # 如果最后一个值与当前值相同，跳过（避免高频重复数据污染历史窗口）
        if self.values and self.values[-1] == float(v):
            return
        
        self.values.append(float(v))
        overflow = len(self.values) - self.max_len
        if overflow > 0:
            # 丢弃最旧的 overflow 个样本
            del self.values[0:overflow]


@dataclass
class FundingFeatureRow:
    """
    Funding 因子族输出（FeatureEngine 写入 FeatureState 的最小单元）。
    """
    funding_rate_raw: Optional[float] = None
    hours_to_settlement: Optional[float] = None
    funding_bucket: Optional[str] = None
    funding_percentile: Optional[float] = None
    funding_z: Optional[float] = None
    funding_extreme_flag: bool = False


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _select_bucket_cfg(
    config: FundingConfig,
    symbol: str,
) -> FundingBucketsConfig:
    """
    获取指定 symbol 的分档配置，若无覆盖则使用默认。
    """
    if config.buckets_per_symbol and symbol in config.buckets_per_symbol:
        return config.buckets_per_symbol[symbol]
    return config.buckets_default


def _bucket_for_rate(
    rate: float,
    buckets_cfg: FundingBucketsConfig,
) -> Optional[str]:
    """
    根据 funding_rate_raw 和阈值配置生成 5 档 bucket 标签。

    档位划分规则（参考产品文档）：
        ≤ neg_extreme          → NEG_EXTREME
        (neg_extreme,neg_high] → NEG_HIGH
        (neg_high,pos_high)    → NEUTRAL
        [pos_high,pos_extreme) → POS_HIGH
        ≥ pos_extreme          → POS_EXTREME
    """
    if not _is_finite_number(rate):
        return None

    r = float(rate)

    if r <= buckets_cfg.neg_extreme:
        return "NEG_EXTREME"
    if r <= buckets_cfg.neg_high:
        return "NEG_HIGH"
    if r < buckets_cfg.pos_high:
        return "NEUTRAL"
    if r < buckets_cfg.pos_extreme:
        return "POS_HIGH"
    return "POS_EXTREME"


def compute_hours_to_settlement(
    next_funding_time_ms: Optional[int],
    now_ts_ms: Optional[int],
) -> Optional[float]:
    """
    计算 hours_to_settlement（单位：小时，限制在 [0,8]）。

    公式（来自产品文档）：
        hours_to_settlement = clamp(
            (next_funding_time - now_ts_ms) / (1000 * 3600),
            0, 8
        )
    """
    if next_funding_time_ms is None or now_ts_ms is None:
        return None

    try:
        delta_ms = next_funding_time_ms - now_ts_ms
        hours = delta_ms / (1000.0 * 3600.0)
        return float(_clamp(hours, 0.0, 8.0))
    except Exception:
        logger.exception(
            "[Funding] failed to compute hours_to_settlement: "
            "next_funding_time_ms=%s, now_ts_ms=%s",
            next_funding_time_ms,
            now_ts_ms,
        )
        return None


def compute_funding_features(
    symbol: str,
    funding_rate_raw: Optional[float],
    next_funding_time_ms: Optional[int],
    now_ts_ms: int,
    config: FundingConfig,
    history: Optional[FundingHistoryState] = None,
    update_history: bool = True,
) -> FundingFeatureRow:
    """
    Funding 因子主计算函数（核心接口）。

    参数：
        symbol:
            交易对，如 "BTCUSDT"。
        funding_rate_raw:
            当前 8 小时 funding 费率（原始值，非年化）。
            来自 /fapi/v1/premiumIndex 的 lastFundingRate。
        next_funding_time_ms:
            下一个结算时间（毫秒 timestamp），来自 premiumIndex。
        now_ts_ms:
            当前 tick 的"参考时间"，建议用 exchange_time_ms 或对齐后的系统时间。
        config:
            FundingConfig 实例，提供 thresholds / extreme_percentile 等。
        history:
            FundingHistoryState 实例，包含历史 funding_rate_raw 序列。
            若为 None，则不计算 percentile / z / extreme_flag。
        update_history:
            是否在函数末尾自动将当前 funding_rate_raw 追加到 history 中。
            默认 True。若上层有更精细的控制，可以设置为 False 并自行维护。

    返回：
        FundingFeatureRow
    """
    # --- 基础字段 ---
    hours_to_settlement = compute_hours_to_settlement(
        next_funding_time_ms=next_funding_time_ms,
        now_ts_ms=now_ts_ms,
    )

    bucket: Optional[str] = None
    funding_percentile: Optional[float] = None
    funding_z: Optional[float] = None
    funding_extreme_flag: bool = False

    # --- bucket 计算 ---
    if _is_finite_number(funding_rate_raw):
        try:
            bucket_cfg = _select_bucket_cfg(config, symbol)
            bucket = _bucket_for_rate(float(funding_rate_raw), bucket_cfg)
        except Exception:
            logger.exception(
                "[Funding] failed to compute bucket for symbol=%s, rate=%s",
                symbol,
                funding_rate_raw,
            )
            bucket = None

    # --- percentile / z / extreme_flag 计算 ---
    # 历史数据不足 / 缺失 → 这部分全 None/False（由上层决定是否使用）
    if (
        history is not None
        and _is_finite_number(funding_rate_raw)
        and len(history.values) >= config.min_history_size
    ):
        try:
            current = float(funding_rate_raw)
            hist_values = history.values

            # 经验分位数
            funding_percentile = _compute_empirical_percentile(
                hist_values,
                current,
            )

            # Z-score
            funding_z = _compute_z_score(
                hist_values,
                current,
                std_floor=config.std_floor,
            )

            # 极端标记（双尾，正负两端都视为极端）
            if funding_percentile is not None:
                p = funding_percentile
                if (
                    p >= config.extreme_percentile
                    or p <= 1.0 - config.extreme_percentile
                ):
                    funding_extreme_flag = True
                else:
                    funding_extreme_flag = False
            else:
                funding_extreme_flag = False

        except Exception:
            logger.exception(
                "[Funding] failed to compute percentile/z/extreme_flag "
                "for symbol=%s",
                symbol,
            )
            funding_percentile = None
            funding_z = None
            funding_extreme_flag = False
    else:
        # history 缺失或样本不足，保持 None/False
        if history is not None and _is_finite_number(funding_rate_raw):
            # 可以在 debug 模式下打印一行，方便诊断
            logger.debug(
                "[Funding] insufficient history for percentile/z: "
                "symbol=%s, history_len=%d, min_history_size=%d",
                symbol,
                len(history.values),
                config.min_history_size,
            )

    # --- history 追加当前样本（可选） ---
    if (
        history is not None
        and update_history
        and _is_finite_number(funding_rate_raw)
    ):
        try:
            history.add(float(funding_rate_raw))
        except Exception:
            logger.exception(
                "[Funding] failed to update history for symbol=%s", symbol
            )

    return FundingFeatureRow(
        funding_rate_raw=float(funding_rate_raw)
        if _is_finite_number(funding_rate_raw)
        else None,
        hours_to_settlement=hours_to_settlement,
        funding_bucket=bucket,
        funding_percentile=funding_percentile,
        funding_z=funding_z,
        funding_extreme_flag=funding_extreme_flag,
    )


__all__ = [
    "FundingBucketsConfig",
    "FundingConfig",
    "FundingHistoryState",
    "FundingFeatureRow",
    "compute_funding_features",
    "compute_hours_to_settlement",
]

