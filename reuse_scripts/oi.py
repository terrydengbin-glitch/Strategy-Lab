# -*- coding: utf-8 -*-
"""
OI Factor Engine - Task R0-1-2 V1.2

Open Interest 因子核心计算模块（Event-Driven 版本）

功能：
- 维护 OI 历史窗口，计算：
  - oi_raw: 当前 OI
  - oi_z:   OI Z-score（滚动窗口）
  - oi_pct_change: 相对锚点 OI 的百分比变化
  - oi_percentile: OI 在历史窗口中的经验分位数
  - oi_quadrant:   OI × Price 象限 (Q1~Q4)
- 支持 delta_window（按样本数 / 时间窗口）的 ΔPrice / ΔOI 计算
- 纯计算，无 I/O 操作；由上层负责调用节奏（建议 Event-Driven：仅新 OI 样本来时调用）

使用约定：
- Harvester/FeatureEngine 层负责：
  - 控制调用频率：只在 open_interest_ts_ms 更新时调用 update()
  - 传入 ts_ms / price / oi
- 本模块不关心 symbol / regime，只做数值计算。

"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Optional, Deque, Dict, Any, Tuple, List

import math
import logging

logger = logging.getLogger(__name__)
logger.propagate = True


__all__ = ["OIConfig", "OIEngine"]


@dataclass
class OIConfig:
    """
    OI 因子配置

    说明：
    - z_window / percentile_window：以"样本数"为单位的窗口大小
    - min_samples：Z-score / percentile 至少需要的样本数
    - std_floor：标准差下限，避免除零
    - max_history：最大历史样本数，防止内存膨胀
    - eps：数值稳定用的下限
    - delta_window_samples：按样本数定义的 Delta 窗口
      - 1 表示对比上一笔（兼容旧逻辑，不推荐用于生产）
      - >1 表示对比 N 样本之前
    - delta_window_ms：按时间定义的 Delta 窗口（毫秒）
      - >0 时优先尝试时间基锚点
      - 两者都配置时：时间窗口优先，样本窗口作为回退
    """
    # Z-score 窗口（样本数）
    z_window: int = 3600
    # 分位数窗口（样本数）
    percentile_window: int = 3600
    # 暖启动所需最小样本数
    min_samples: int = 100
    # 标准差下限
    std_floor: float = 1e-6
    # 历史上限，防止内存膨胀
    max_history: int = 5000
    # 防除零
    eps: float = 1e-9

    # Delta 相关参数
    # 以"样本数"计的 Delta 窗口大小；
    # 1 表示对比上一笔（保持与测试兼容），生产环境建议 > 1
    delta_window_samples: int = 1
    # 按时间窗口（毫秒），0 表示关闭
    delta_window_ms: int = 0


class OIEngine:
    """
    OI 因子计算引擎（事件驱动）

    update(ts_ms, price, oi) 仅在有"新 OI 样本"时被调用：
    - ts_ms:   OI 样本发生的时间（通常是 open_interest_ts_ms）
    - price:   对应时刻的价格（mid / last 均可，调用方决定）
    - oi:      当前 OI（float）

    返回字典：
        {
          "oi_raw": float | None,
          "oi_z": float | None,
          "oi_pct_change": float | None,
          "oi_percentile": float | None,
          "oi_quadrant": str | None,
          "warmup": bool,
          "meta": {
              "n": int,          # 有效样本数
              "std_zero": bool,  # 标准差是否触及 std_floor
          },
        }
    """

    def __init__(self, cfg: Optional[OIConfig] = None) -> None:
        self.cfg: OIConfig = cfg or OIConfig()

        # OI 历史，用于 Z-score 计算
        self._oi_hist: Deque[float] = deque(maxlen=self.cfg.z_window)

        # OI 历史，用于分位数计算（可以与 _oi_hist 重用，但拆分更清晰）
        self._oi_window_for_pct: Deque[float] = deque(maxlen=self.cfg.percentile_window)

        # 带时间戳/价格/oi 的完整历史，用于 Delta & 象限
        # 每个元素为 (ts_ms, price, oi)
        self._history: Deque[Tuple[int, Optional[float], float]] = deque(maxlen=self.cfg.max_history)

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------
    def update(
        self,
        ts_ms: int,
        price: Optional[float],
        oi: Optional[float],
    ) -> Dict[str, Any]:
        """
        处理一条新的 OI 样本（Event-Driven 调用）

        注意：
        - 若 oi 为 None，则不更新内部状态，返回 warmup=True + 全 None
          （预期上层不会频繁传入 None）
        """
        if oi is None:
            # 不更新内部状态，只返回占位输出
            n_samples = len(self._oi_hist)
            return {
                "oi_raw": None,
                "oi_z": None,
                "oi_pct_change": None,
                "oi_percentile": None,
                "oi_quadrant": None,
                "warmup": True,
                "meta": {
                    "n": n_samples,
                    "std_zero": False,
                },
            }

        # 1) 更新内部窗口
        self._oi_hist.append(oi)
        self._oi_window_for_pct.append(oi)
        self._history.append((ts_ms, price, oi))

        n_samples = len(self._oi_hist)

        # 2) 计算 Z-score
        oi_z, std_zero = self._compute_zscore(oi)

        # 3) 计算 Percentile（O(N) 实现）
        oi_percentile = self._compute_percentile(oi)

        # 4) 计算 Delta & 象限（基于滚动锚点）
        anchor_price, anchor_oi = self._select_anchor(ts_ms)
        oi_pct_change: Optional[float] = None
        oi_quadrant: Optional[str] = None

        if anchor_oi is not None:
            # 百分比变化：相对锚点
            denom = abs(anchor_oi) if abs(anchor_oi) > self.cfg.eps else self.cfg.eps
            oi_pct_change = (oi - anchor_oi) / denom

            # 象限：需要价格和 OI
            if price is not None:
                oi_quadrant = self._compute_quadrant(
                    anchor_price=anchor_price,
                    anchor_oi=anchor_oi,
                    price=price,
                    oi=oi,
                )

        # 5) 暖启动判定
        warmup = n_samples < self.cfg.min_samples

        return {
            "oi_raw": oi,
            "oi_z": oi_z,
            "oi_pct_change": oi_pct_change,
            "oi_percentile": oi_percentile,
            "oi_quadrant": oi_quadrant,
            "warmup": warmup,
            "meta": {
                "n": n_samples,
                "std_zero": bool(std_zero),
            },
        }

    # ------------------------------------------------------------------
    # 内部：Z-score
    # ------------------------------------------------------------------
    def _compute_zscore(self, oi: float) -> Tuple[Optional[float], bool]:
        """
        基于 _oi_hist 计算当前样本的 Z-score

        返回:
            (z, std_zero)
            - z:        当前 OI 的 Z-score（可能为 None，若样本不足）
            - std_zero: 标准差是否被 std_floor 限制
        """
        window = list(self._oi_hist)[-self.cfg.z_window :]
        n = len(window)
        if n < self.cfg.min_samples:
            return None, False

        # 计算均值 & 方差
        mean_val = sum(window) / float(n)
        var = sum((x - mean_val) ** 2 for x in window) / float(n)
        std = math.sqrt(var) if var > 0.0 else 0.0

        std_zero = False
        # P1-FIX: 确保 std_floor 是浮点数类型（防御性类型转换）
        # 如果配置文件中 std_floor 被解析为字符串（如 "1e-6"），需要转换
        try:
            std_floor_float = float(self.cfg.std_floor)
        except (ValueError, TypeError) as e:
            logger.warning(
                f"[OI] std_floor 类型转换失败: {self.cfg.std_floor} (type={type(self.cfg.std_floor)}), "
                f"使用默认值 1e-6, 错误: {e}"
            )
            std_floor_float = 1e-6
        
        if std < std_floor_float:
            std_zero = True
            std = std_floor_float

        z = (oi - mean_val) / std if std > 0 else 0.0
        return z, std_zero

    # ------------------------------------------------------------------
    # 内部：Percentile (O(N))
    # ------------------------------------------------------------------
    def _compute_percentile(self, oi: float) -> Optional[float]:
        """
        O(N) 复杂度的分位数计算（经验分布）

        定义：
            percentile = count_smaller / (n - 1)
        说明：
            - 使用 _oi_window_for_pct 中的最近样本
            - 样本数不足 min_samples 时返回 None
        """
        n = min(len(self._oi_window_for_pct), self.cfg.percentile_window)
        if n <= 1 or n < self.cfg.min_samples:
            return None

        # 遍历窗口统计小于当前值的数量
        count_smaller = 0
        for x in self._oi_window_for_pct:
            if x < oi:
                count_smaller += 1

        return count_smaller / float(n - 1) if n > 1 else 0.0

    # ------------------------------------------------------------------
    # 内部：锚点选择 (Delta Window)
    # ------------------------------------------------------------------
    def _select_anchor(self, ts_ms: int) -> Tuple[Optional[float], Optional[float]]:
        """
        根据 delta_window_samples / delta_window_ms 选择锚点样本。

        策略：
        1. 若配置了 delta_window_ms > 0：
           - 从 history 中（排除当前样本）向前搜索：
             找到"距离当前时间 >= delta_window_ms"的最新样本，作为锚点。
        2. 否则若 delta_window_samples > 1：
           - 使用 N 样本之前的样本作为锚点：
             idx = max(0, n - delta_window_samples)
        3. 若以上都不可用，则回退到"上一笔样本"：
           - n >= 2 时，anchor = history[n - 2]
           - 否则返回 (None, None)
        """
        n = len(self._history)
        if n == 0:
            return None, None

        idx: Optional[int] = None

        # 1) 时间窗口优先
        if self.cfg.delta_window_ms > 0 and n >= 2:
            target_ms = self.cfg.delta_window_ms
            # 从倒数第二个样本开始向前找
            for j in range(n - 2, -1, -1):
                ts_j, _, _ = self._history[j]
                dt = ts_ms - ts_j
                if dt >= target_ms:
                    idx = j
                    break

        # 2) 若时间窗口未命中，再使用样本窗口
        if idx is None and self.cfg.delta_window_samples > 1:
            # N 样本之前
            idx = max(0, n - self.cfg.delta_window_samples)

        # 3) 仍然没有，则退回上一笔
        if idx is None:
            if n >= 2:
                idx = n - 2
            else:
                return None, None

        anchor_ts, anchor_price, anchor_oi = self._history[idx]
        # ts 不用于后续，仅作为调试参考
        _ = anchor_ts
        return anchor_price, anchor_oi

    # ------------------------------------------------------------------
    # 内部：象限计算
    # ------------------------------------------------------------------
    def _compute_quadrant(
        self,
        anchor_price: Optional[float],
        anchor_oi: float,
        price: Optional[float],
        oi: float,
    ) -> Optional[str]:
        """
        根据 ΔPrice / ΔOI 计算象限：
            Q1: ΔPrice > 0, ΔOI > 0
            Q2: ΔPrice > 0, ΔOI < 0
            Q3: ΔPrice < 0, ΔOI < 0
            Q4: ΔPrice < 0, ΔOI > 0

        若 ΔPrice / ΔOI 绝对值均小于 eps，则返回 None（视为噪音）。
        """
        if anchor_price is None or price is None:
            return None

        d_price = price - anchor_price
        d_oi = oi - anchor_oi

        if abs(d_price) < self.cfg.eps and abs(d_oi) < self.cfg.eps:
            return None

        if d_price > 0 and d_oi > 0:
            return "Q1"
        if d_price > 0 and d_oi < 0:
            return "Q2"
        if d_price < 0 and d_oi < 0:
            return "Q3"
        if d_price < 0 and d_oi > 0:
            return "Q4"

        # 某一维接近 0 的情况统一视为无象限
        return None

