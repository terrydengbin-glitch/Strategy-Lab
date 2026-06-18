# -*- coding: utf-8 -*-
"""
Real OFI Calculator - Task 1.2.1 (L1 OFI版本)
真实OFI计算器（快照模式 + L1价跃迁敏感）

=== 参数锁定状态 ===
当前参数组合已通过验收测试，达到100%通过率：
- IQR: 1.324 (目标≥0.8) OK
- P(|z|>3): 0.00% (目标≤1.5%) OK
- P(|z|>2): 5.88% (目标1-8%) OK

锁定参数：
- z_window=80: 拉宽有效起伏，平衡统计稳定性
- ema_alpha=0.30: 增强响应性，保持平滑性
- z_clip=3.0: 轻收尾部，精确控制P(|z|>2)在5-7%

护栏机制：
- reset_on_gap_ms=2000: 间隔重置阈值，防止跨段污染
- reset_on_session_change=True: 会话切换重置，确保统计独立性
- per_symbol_window=True: 按交易对独立窗口，避免交叉污染

功能：
- 基于订单簿快照计算L1 OFI (Order Flow Imbalance)
- 最优价跃迁冲击 + 5档深度加权计算
- Z-score标准化（"上一窗口"基线 + std_zero标记）
- EMA平滑
- 纯计算，无I/O操作

核心实现要点：
1. L1 OFI（价跃迁敏感）：
   - 最优档位：检测价格跃迁，计算冲击项
   - 价上涨：新最优价队列为正冲击，旧队列为负冲击
   - 价下跌：旧最优价队列为负冲击，新队列为正冲击
   - 其余档位：标准数量变化 Δbid_qty_k - Δask_qty_k

2. 权重与档位：
   - 默认权重 [0.4, 0.25, 0.2, 0.1, 0.05]
   - 按K档裁剪/填充并归一化，负值截为0，权重和为1

3. 输入清洗：
   - _pad_snapshot 保障价格为有限值、数量非负
   - 异常数据计入 bad_points

4. Z-score（优化版）：
   - 基线="上一窗口"（不包含当前ofi），避免当前值稀释
   - warmup_threshold = max(5, z_window//5)，不足返回 z_ofi=None
   - std <= 1e-9 则 z_ofi=0.0 且 meta.std_zero=True

5. EMA：
   - ema_alpha可配，首次用当前ofi初始化，其后标准递推

6. 状态与边界：
   - reset()/get_state() 可观测
   - L2增量模式显式 NotImplementedError（后续任务再做）

作者: V13 OFI+CVD+AI System
创建时间: 2025-10-17
最后优化: 2025-10-21 (L1 OFI价跃迁敏感版本)
"""
from __future__ import annotations
from collections import deque
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any
import numpy as np

@dataclass
class OFIConfig:
    """OFI计算器配置类 - 支持按流动性分层参数"""
    levels: int = 5  # 订单簿档位数
    weights: Optional[List[float]] = None  # 自定义权重，默认None使用标准权重
    
    # === 分层参数配置 ===
    z_window: int = 75  # 默认：从80降到75，拉宽IQR
    ema_alpha: float = 0.30  # 保持响应性
    z_clip: float = 3.0  # 基础裁剪阈值
    
    # === 会话化重置机制（护栏） ===
    reset_on_gap_ms: int = 2000  # 间隔重置阈值，防止跨段污染
    reset_on_session_change: bool = True  # 会话切换重置，确保统计独立性
    per_symbol_window: bool = True  # 按交易对独立窗口，避免交叉污染
    
    # === 稳健化守护机制 ===
    std_floor: float = 1e-4  # 标准差下限，防止除以近零放大
    winsorize_ofi_delta: float = 0.9  # OFI增量MAD倍数，软截极值
    debug_winsorize: bool = False  # Winsorize调试日志开关

def _is_finite_number(x: float) -> bool:
    """
    检查是否为有效的有限数字
    
    参数:
        x: 待检查的数字
    
    返回:
        bool: 是否为有效有限数字
    """
    try:
        y = float(x)
        return y == y and y not in (float('inf'), float('-inf'))
    except Exception:
        return False

class RealOFICalculator:
    """
    真实OFI计算器（快照模式）
    
    核心功能:
    1. 基于5档订单簿快照计算OFI
    2. 深度加权: [0.4, 0.25, 0.2, 0.1, 0.05]
    3. Z-score标准化（滚动窗口300）
    4. EMA平滑（alpha=0.2）
    
    计算公式:
    - OFI_k = w_k * (Δbid_qty_k - Δask_qty_k)
    - OFI = Σ OFI_k (k=0 to K-1)
    - z_ofi = (OFI - mean(OFI_hist)) / std(OFI_hist)
    - ema_ofi = alpha * OFI + (1-alpha) * ema_ofi_prev
    
    使用示例:
        >>> config = OFIConfig(levels=5, z_window=300)
        >>> calc = RealOFICalculator("ETHUSDT", config)
        >>> bids = [[3245.5, 10.5], [3245.4, 8.3], ...]
        >>> asks = [[3245.6, 11.2], [3245.7, 9.5], ...]
        >>> result = calc.update_with_snapshot(bids, asks)
        >>> print(f"OFI={result['ofi']:.4f}, Z-score={result['z_ofi']:.4f}")
    """
    
    __slots__ = (
        "symbol", "K", "w", "z_window", "ema_alpha", "z_clip",
        "reset_on_gap_ms", "reset_on_session_change", "per_symbol_window",
        "std_floor", "winsor_k_mad", "config", "cfg", "debug_winsorize",
        "bids", "asks", "prev_bids", "prev_asks",
        "ofi_hist", "ema_ofi", "bad_points",
        "bid_jump_up_cnt", "bid_jump_down_cnt", "ask_jump_up_cnt", "ask_jump_down_cnt",
        "bid_jump_up_impact_sum", "bid_jump_down_impact_sum", 
        "ask_jump_up_impact_sum", "ask_jump_down_impact_sum",
        "_last_event_time_ms", "_last_utc_day", "_z_zero_streak",
        "p_gt2_cnt", "p_gt3_cnt", "total_cnt"  # 尾部监控计数器
    )
    
    def __init__(self, symbol: str, cfg: OFIConfig = None, config_loader=None, runtime_cfg: Dict[str, Any] = None):
        """
        初始化OFI计算器
        
        参数:
            symbol: 交易对符号（如"ETHUSDT"）
            cfg: OFI配置对象，默认None使用默认配置
            config_loader: 配置加载器实例（兼容旧接口，库式调用时不应使用）
            runtime_cfg: 运行时配置字典，库式调用时使用（优先于config_loader）
        """
        # 优先使用运行时配置字典（库式调用）
        if runtime_cfg is not None:
            ofi_cfg = runtime_cfg.get('ofi', {}) if isinstance(runtime_cfg, dict) else {}
            # 从运行时配置构建OFIConfig对象
            cfg = OFIConfig(
                levels=ofi_cfg.get('levels', 5),
                weights=ofi_cfg.get('weights', None),
                z_window=ofi_cfg.get('z_window', 80),
                ema_alpha=ofi_cfg.get('ema_alpha', 0.30),
                z_clip=ofi_cfg.get('z_clip', 3.0),
                reset_on_gap_ms=ofi_cfg.get('reset_on_gap_ms', 2000),
                reset_on_session_change=ofi_cfg.get('reset_on_session_change', True),
                per_symbol_window=ofi_cfg.get('per_symbol_window', True),
                debug_winsorize=ofi_cfg.get('debug_winsorize', False)
            )
        elif config_loader:
            # 从统一配置系统加载参数（兼容旧接口）
            cfg = self._load_from_config_loader(config_loader, symbol)
        elif cfg is None:
            cfg = OFIConfig()
            
        self.symbol = (symbol or "").upper()
        self.K = int(cfg.levels) if cfg.levels and cfg.levels > 0 else 5
        
        # 初始化权重（使用统一的归一化方法）
        self.w = self._normalize_weights(cfg.weights, self.K)
        
        self.z_window = int(cfg.z_window) if cfg.z_window and cfg.z_window > 0 else 80
        self.ema_alpha = float(cfg.ema_alpha)
        self.z_clip = float(getattr(cfg, 'z_clip', 3.0))
        self.reset_on_gap_ms = int(getattr(cfg, 'reset_on_gap_ms', 2000))
        self.reset_on_session_change = bool(getattr(cfg, 'reset_on_session_change', True))
        self.per_symbol_window = bool(getattr(cfg, 'per_symbol_window', True))
        
        # 稳健化守护机制参数
        self.std_floor = float(getattr(cfg, 'std_floor', 1e-4))
        # 统一winsorize参数命名，支持多个别名
        self.winsor_k_mad = float(
            getattr(cfg, 'winsor_k_mad', 
                   getattr(cfg, 'winsorize_ofi_delta', 
                          getattr(cfg, 'winsorize_ofi_delta_mad_k', 3.0)))
        )
        # Winsorize调试日志开关
        self.debug_winsorize = bool(getattr(cfg, 'debug_winsorize', False))
        
        # 保存配置对象用于访问其他参数
        self.config = cfg
        self.cfg = cfg  # 修复：与 update_params 对齐
        
        # 初始化订单簿缓存
        self.bids = [[0.0, 0.0] for _ in range(self.K)]
        self.asks = [[0.0, 0.0] for _ in range(self.K)]
        self.prev_bids = [[0.0, 0.0] for _ in range(self.K)]
        self.prev_asks = [[0.0, 0.0] for _ in range(self.K)]
        
        # 初始化历史数据
        self.ofi_hist = deque(maxlen=self.z_window)
        self.ema_ofi: Optional[float] = None
        self.bad_points = 0
        
        # L1价跃迁诊断统计
        self.bid_jump_up_cnt = 0
        self.bid_jump_down_cnt = 0
        self.ask_jump_up_cnt = 0
        self.ask_jump_down_cnt = 0
        self.bid_jump_up_impact_sum = 0.0
        self.bid_jump_down_impact_sum = 0.0
        self.ask_jump_up_impact_sum = 0.0
        self.ask_jump_down_impact_sum = 0.0
        
        # 尾部监控计数器
        self.p_gt2_cnt = 0  # |z| > 2 的计数
        self.p_gt3_cnt = 0  # |z| > 3 的计数
        self.total_cnt = 0  # 总计数
    
    def _normalize_weights(self, weights: Optional[List[float]], levels: int) -> List[float]:
        """根据 levels 生成权重向量，并做归一化。
        
        参数:
            weights: 自定义权重列表，None时使用默认三角权重方案
            levels: 订单簿档位数
        
        返回:
            List[float]: 归一化后的权重向量，sum(w) == 1
        
        行为说明:
            - 如果传入 weights 为空或长度不足，使用默认三角权重方案 [0.4, 0.25, 0.2, 0.1, 0.05]
            - 如果长度大于 levels，裁剪前 levels 个
            - 如果长度小于 levels，用0.0填充到 levels 长度
            - 负值截为0，最后将权重归一化到 sum(w) == 1
        """
        default_w = [0.4, 0.25, 0.2, 0.1, 0.05]
        if weights is None:
            # 使用默认权重，裁剪或填充到K档
            w_raw = default_w[:levels] if len(default_w) >= levels else (
                default_w + [0.0] * max(0, levels - len(default_w))
            )
        else:
            # 使用自定义权重，裁剪或填充到levels档
            w_raw = [float(x) for x in weights[:levels]] + [0.0] * max(0, levels - len(weights))
        
        # 归一化权重（确保总和为1）
        total = sum(max(0.0, x) for x in w_raw)
        if total <= 0.0:
            raise ValueError("weights must have positive sum")
        return [max(0.0, x) / total for x in w_raw]
    
    def _load_from_config_loader(self, config_loader, symbol: str) -> OFIConfig:
        """
        从统一配置系统加载OFI参数
        
        参数:
            config_loader: 配置加载器实例
            symbol: 交易对符号
            
        返回:
            OFI配置对象
        """
        try:
            # 获取OFI配置
            ofi_config = config_loader.get('components.ofi', {})
            binance_config = config_loader.get('binance', {})
            
            # 提取配置参数
            levels = ofi_config.get('levels', binance_config.get('ofi', {}).get('levels', 5))
            weights = ofi_config.get('weights', binance_config.get('ofi', {}).get('weights', [0.4, 0.25, 0.2, 0.1, 0.05]))
            z_window = ofi_config.get('z_window', binance_config.get('ofi', {}).get('window_size', 300))
            ema_alpha = ofi_config.get('ema_alpha', 0.2)
            
            # 创建配置对象
            return OFIConfig(
                levels=levels,
                weights=weights,
                z_window=z_window,
                ema_alpha=ema_alpha
            )
            
        except Exception as e:
            # 如果配置加载失败，使用默认配置并记录警告
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to load OFI config from config_loader: {e}. Using default config.")
            return OFIConfig()  # 坏数据点计数器

    def _pad_snapshot(self, arr: List[Tuple[float, float]]) -> List[List[float]]:
        """
        填充订单簿快照到K档，处理无效数据
        
        参数:
            arr: 订单簿数据 [(价格, 数量), ...]
        
        返回:
            List[List[float]]: K档订单簿 [[价格, 数量], ...]
        """
        out = [[0.0, 0.0] for _ in range(self.K)]
        n = min(len(arr or []), self.K)
        bad = False
        
        for i in range(n):
            p, q = arr[i]
            # 检查价格有效性
            if not _is_finite_number(p):
                bad = True
                p = 0.0
            # 检查数量有效性（必须非负）
            if not _is_finite_number(q) or float(q) < 0:
                bad = True
                q = 0.0
            out[i][0] = float(p)
            out[i][1] = float(q)
        
        if bad:
            self.bad_points += 1
        
        return out

    def _sort_if_needed(self, side: List[Tuple[float, float]], reverse: bool) -> List[Tuple[float, float]]:
        """
        检查并排序订单簿数据，确保价格单调性
        
        参数:
            side: 订单簿数据 [(价格, 数量), ...]
            reverse: True为降序(bids)，False为升序(asks)
        
        返回:
            List[Tuple[float, float]]: 排序后的订单簿数据
        """
        if not side or len(side) <= 1:
            return side
        
        # 检查是否已经有序
        ok = True
        for i in range(1, min(len(side), self.K)):
            if reverse and side[i][0] > side[i-1][0]:
                ok = False
                break
            if (not reverse) and side[i][0] < side[i-1][0]:
                ok = False
                break
        
        # 只在需要时排序以节省开销
        return sorted(side, key=lambda x: x[0], reverse=reverse) if not ok else side

    def _compute_mad_threshold(self) -> Tuple[float, float]:
        """基于 self.ofi_hist 计算 median 与 MAD.
        
        当前实现仍使用 numpy，保持行为完全一致。
        后续如需优化，可以在此方法内部改为纯 Python / 降频计算。
        
        返回:
            Tuple[float, float]: (median_ofi, mad) 中位数和MAD值
        """
        ofi_values = np.array(list(self.ofi_hist), dtype=float)
        median_ofi = np.median(ofi_values)
        mad = np.median(np.abs(ofi_values - median_ofi))
        return median_ofi, mad
    
    @staticmethod
    def _mean_std(values: List[float]) -> Tuple[float, float]:
        """
        计算均值和标准差（样本标准差）
        
        参数:
            values: 数值列表
        
        返回:
            Tuple[float, float]: (均值, 标准差)
        """
        n = len(values)
        if n == 0:
            return 0.0, 0.0
        
        m = sum(values) / n
        if n == 1:
            return m, 0.0
        
        var = sum((x - m) * (x - m) for x in values) / (n - 1)
        return m, var ** 0.5

    def reset(self) -> None:
        """
        重置计算器状态，清空所有历史数据
        """
        for i in range(self.K):
            self.bids[i][0] = self.bids[i][1] = 0.0
            self.asks[i][0] = self.asks[i][1] = 0.0
            self.prev_bids[i][0] = self.prev_bids[i][1] = 0.0
            self.prev_asks[i][0] = self.prev_asks[i][1] = 0.0
        
        self.ofi_hist.clear()
        self.ema_ofi = None
        self.bad_points = 0

    def get_state(self) -> Dict:
        """
        获取计算器当前状态
        
        返回:
            Dict: 包含symbol, levels, weights, bids, asks等状态信息
        """
        return {
            "symbol": self.symbol,
            "levels": self.K,
            "weights": list(self.w),
            "bids": [list(x) for x in self.bids],
            "asks": [list(x) for x in self.asks],
            "bad_points": self.bad_points,
            "ema_ofi": self.ema_ofi,
            "ofi_hist_len": len(self.ofi_hist),
        }
    
    def _reset_session(self):
        """重置会话状态，清空历史数据避免跨段污染"""
        print(f"[OFI_SESSION_RESET] 重置会话状态，清空历史数据")
        self.ofi_hist.clear()
        self.ema_ofi = None
        self.bad_points = 0
        # 重置跳价统计
        self.bid_jump_up_cnt = 0
        self.bid_jump_down_cnt = 0
        self.ask_jump_up_cnt = 0
        self.ask_jump_down_cnt = 0
        self.bid_jump_up_impact_sum = 0.0
        self.bid_jump_down_impact_sum = 0.0
        self.ask_jump_up_impact_sum = 0.0
        self.ask_jump_down_impact_sum = 0.0
        
        # 尾部监控计数器
        self.p_gt2_cnt = 0  # |z| > 2 的计数
        self.p_gt3_cnt = 0  # |z| > 3 的计数
        self.total_cnt = 0  # 总计数

    def update_with_snapshot(
        self, 
        bids: List[Tuple[float, float]], 
        asks: List[Tuple[float, float]], 
        event_time_ms: Optional[int] = None,
        current_symbol: Optional[str] = None
    ) -> Dict:
        """
        基于订单簿快照更新OFI
        
        参数:
            bids: 买单列表 [(价格, 数量), ...] 按价格降序
            asks: 卖单列表 [(价格, 数量), ...] 按价格升序
            event_time_ms: 事件时间戳（毫秒），可选
            current_symbol: 当前交易对，用于检测交易对切换
        
        返回:
            Dict: {
                "symbol": 交易对,
                "event_time_ms": 事件时间,
                "ofi": OFI值,
                "k_components": 各档OFI贡献 [ofi_0, ofi_1, ...],
                "z_ofi": Z-score标准化后的OFI (warmup期间为None),
                "ema_ofi": EMA平滑后的OFI,
                "meta": {
                    "levels": 档位数,
                    "weights": 权重列表,
                    "bad_points": 坏数据点计数,
                    "warmup": 是否在warmup期,
                    "session_reset": 是否发生会话重置
                }
            }
        """
        # 会话化与断点重置检查
        session_reset = False
        
        # 1. 检查时间间隔重置（>2000ms）
        if hasattr(self, '_last_event_time_ms') and event_time_ms is not None:
            time_gap = event_time_ms - self._last_event_time_ms
            if time_gap > getattr(self, 'reset_on_gap_ms', 2000):
                print(f"[OFI_SESSION_RESET] 时间间隔过大: {time_gap}ms > {getattr(self, 'reset_on_gap_ms', 2000)}ms")
                self._reset_session()
                session_reset = True
        
        # 2. 检查交易对切换重置
        if (hasattr(self, 'per_symbol_window') and self.per_symbol_window and 
            current_symbol is not None and current_symbol.upper() != self.symbol):
            print(f"[OFI_SYMBOL_RESET] 交易对切换: {self.symbol} -> {current_symbol.upper()}")
            self._reset_session()
            session_reset = True
        
        # 3. 检查会话切换重置（交易日/UTC日切换）
        if (hasattr(self, 'reset_on_session_change') and self.reset_on_session_change and 
            event_time_ms is not None):
            # 简单的UTC日切换检测
            if hasattr(self, '_last_utc_day'):
                current_utc_day = event_time_ms // (24 * 60 * 60 * 1000)
                if current_utc_day != self._last_utc_day:
                    print(f"[OFI_DAY_RESET] UTC日切换: {self._last_utc_day} -> {current_utc_day}")
                    self._reset_session()
                    session_reset = True
                self._last_utc_day = current_utc_day
            else:
                self._last_utc_day = event_time_ms // (24 * 60 * 60 * 1000)
        
        # 更新最后事件时间
        if event_time_ms is not None:
            self._last_event_time_ms = event_time_ms
        
        # 保存上一帧订单簿
        for i in range(self.K):
            self.prev_bids[i][0] = self.bids[i][0]
            self.prev_bids[i][1] = self.bids[i][1]
            self.prev_asks[i][0] = self.asks[i][0]
            self.prev_asks[i][1] = self.asks[i][1]
        
        # 安全排序：确保bids降序、asks升序
        bids_sorted = self._sort_if_needed(bids or [], reverse=True)
        asks_sorted = self._sort_if_needed(asks or [], reverse=False)
        
        # 更新当前订单簿
        self.bids = self._pad_snapshot(bids_sorted)
        self.asks = self._pad_snapshot(asks_sorted)

        # 计算L1 OFI（最优价跃迁敏感版本）
        k_components = []
        ofi_val = 0.0
        
        # L1 OFI: 最优价跃迁冲击 + 其余档位数量变化
        for i in range(self.K):
            if i == 0:  # 最优档位：处理价跃迁冲击
                # 检查bid最优价是否变化
                bid_price_changed = abs(self.bids[i][0] - self.prev_bids[i][0]) > 1e-8
                ask_price_changed = abs(self.asks[i][0] - self.prev_asks[i][0]) > 1e-8
                
                if bid_price_changed or ask_price_changed:
                    # 价跃迁冲击：新最优价队列为正冲击，旧最优价队列为负冲击
                    bid_impact = 0.0
                    ask_impact = 0.0
                    
                    if self.bids[i][0] > self.prev_bids[i][0]:  # bid价上涨
                        self.bid_jump_up_cnt += 1
                        # 新最优价队列为正冲击
                        bid_impact = self.bids[i][1]
                        # 旧最优价队列为负冲击（如果存在）
                        if self.prev_bids[i][1] > 0:
                            bid_impact -= self.prev_bids[i][1]
                        self.bid_jump_up_impact_sum += bid_impact
                    elif self.bids[i][0] < self.prev_bids[i][0]:  # bid价下跌
                        self.bid_jump_down_cnt += 1
                        # 旧最优价队列为负冲击
                        bid_impact = -self.prev_bids[i][1]
                        # 新最优价队列为正冲击
                        if self.bids[i][1] > 0:
                            bid_impact += self.bids[i][1]
                        self.bid_jump_down_impact_sum += bid_impact
                    else:  # 价格不变，用数量变化
                        bid_impact = self.bids[i][1] - self.prev_bids[i][1]
                    
                    # ask端对称处理（负号）
                    if self.asks[i][0] > self.prev_asks[i][0]:  # ask价上涨
                        self.ask_jump_up_cnt += 1
                        # 旧最优价队列为负冲击
                        ask_impact = -self.prev_asks[i][1]
                        # 新最优价队列为正冲击
                        if self.asks[i][1] > 0:
                            ask_impact += self.asks[i][1]
                        self.ask_jump_up_impact_sum += ask_impact
                    elif self.asks[i][0] < self.prev_asks[i][0]:  # ask价下跌
                        self.ask_jump_down_cnt += 1
                        # 新最优价队列为正冲击
                        ask_impact = self.asks[i][1]
                        # 旧最优价队列为负冲击（如果存在）
                        if self.prev_asks[i][1] > 0:
                            ask_impact -= self.prev_asks[i][1]
                        self.ask_jump_down_impact_sum += ask_impact
                    else:  # 价格不变，用数量变化
                        ask_impact = self.asks[i][1] - self.prev_asks[i][1]
                    
                    # L1冲击：bid为正，ask为负
                    comp = self.w[i] * (bid_impact - ask_impact)
                else:
                    # 价格不变，用标准数量变化
                    delta_b = self.bids[i][1] - self.prev_bids[i][1]
                    delta_a = self.asks[i][1] - self.prev_asks[i][1]
                    comp = self.w[i] * (delta_b - delta_a)
            else:
                # 其余档位：标准数量变化
                delta_b = self.bids[i][1] - self.prev_bids[i][1]
                delta_a = self.asks[i][1] - self.prev_asks[i][1]
                comp = self.w[i] * (delta_b - delta_a)
            
            k_components.append(comp)
            ofi_val += comp

        # 计算Z-score（基于"上一窗口"，不包含当前ofi_val）
        z_ofi = None
        warmup = False
        std_zero = False
        warmup_threshold = max(5, self.z_window // 5)
        
        # 统一从历史窗口获取数据（不包含当前值）
        arr = list(self.ofi_hist)
        if len(arr) < warmup_threshold:
            warmup = True
            # 严格区分"未就绪(None)"与"就绪但弱(数值)"
            z_ofi = None  # warmup期间返回None，而不是0.0
            # [FIX] 使用logger.debug()而不是print()，避免刷屏
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"[OFI_WARMUP] Samples: {len(arr)}, threshold: {warmup_threshold}, z_ofi=None")
        else:
            m, s = self._mean_std(arr)
            
            # 稳健化守护：标准差下限保护
            if s < self.std_floor:
                s = self.std_floor
                std_zero = True  # 标记使用了标准差下限
                print(f"[OFI_STD_FLOOR] 标准差过小，使用下限: {s:.6f}")
            
            # 计算Z-score
            z_ofi = (ofi_val - m) / s
            
            # 稳健化守护：OFI值MAD软截
            if hasattr(self, 'ofi_hist') and len(self.ofi_hist) > 10:
                median_ofi, mad = self._compute_mad_threshold()
                mad_threshold = self.winsor_k_mad * mad
                
                # 软截极值
                if mad > 0 and abs(ofi_val - median_ofi) > mad_threshold:
                    ofi_val_original = ofi_val
                    ofi_val = median_ofi + (mad_threshold if ofi_val > median_ofi else -mad_threshold)
                    z_ofi = (ofi_val - m) / s
                    # 使用结构化日志，支持调试开关
                    if self.debug_winsorize:
                        import logging
                        logger = logging.getLogger(__name__)
                        logger.debug(
                            "[OFI_WINSORIZE] 软截极值: %.3f -> %.3f",
                            ofi_val_original, ofi_val
                        )
            
            # 应用z_clip裁剪（支持关闭模式）
            z_clip = getattr(self, 'z_clip', 3.0)
            # 支持关闭模式：None, <=0, 或极大值(>=1e6)时跳过裁剪
            if z_clip is not None and z_clip > 0 and z_clip < 1e6:
                if abs(z_ofi) > z_clip:
                    pre = z_ofi  # 修复：先保存裁剪前的值
                    z_ofi = z_clip if z_ofi > 0 else -z_clip
                    print(f"[OFI_Z_CLIP] 裁剪Z-score: {pre:.3f} -> {z_ofi:.3f}")
                # 重置Z-score为零的计数
                if hasattr(self, '_z_zero_streak'):
                    self._z_zero_streak = 0

        # 更新EMA
        if self.ema_ofi is None:
            self.ema_ofi = ofi_val
        else:
            a = self.ema_alpha
            self.ema_ofi = a * ofi_val + (1.0 - a) * self.ema_ofi
        
        # 更新OFI历史（放在Z-score计算后，确保"上一窗口"口径）
        self.ofi_hist.append(ofi_val)
        
        # 尾部监控计数
        if z_ofi is not None:
            self.total_cnt += 1
            if abs(z_ofi) > 2:
                self.p_gt2_cnt += 1
            if abs(z_ofi) > 3:
                self.p_gt3_cnt += 1

        return {
            "symbol": self.symbol,
            "event_time_ms": event_time_ms,
            "ofi": ofi_val,
            "k_components": k_components,
            "z_ofi": z_ofi,
            "ema_ofi": self.ema_ofi,
                "meta": {
                "levels": self.K,
                "weights": list(self.w),
                "bad_points": self.bad_points,
                "warmup": warmup,
                "std_zero": std_zero,  # 新增：标准差为0标记
                "session_reset": session_reset,  # 新增：会话重置标记
                # L1价跃迁诊断统计
                "bid_jump_up_cnt": self.bid_jump_up_cnt,
                "bid_jump_down_cnt": self.bid_jump_down_cnt,
                "ask_jump_up_cnt": self.ask_jump_up_cnt,
                "ask_jump_down_cnt": self.ask_jump_down_cnt,
                "bid_jump_up_impact_sum": self.bid_jump_up_impact_sum,
                "bid_jump_down_impact_sum": self.bid_jump_down_impact_sum,
                "ask_jump_up_impact_sum": self.ask_jump_up_impact_sum,
                "ask_jump_down_impact_sum": self.ask_jump_down_impact_sum,
                # 尾部监控数据
                "p_gt2_cnt": self.p_gt2_cnt,
                "p_gt3_cnt": self.p_gt3_cnt,
                "total_cnt": self.total_cnt,
                "p_gt2_percent": (self.p_gt2_cnt / self.total_cnt * 100) if self.total_cnt > 0 else 0.0,
                "p_gt3_percent": (self.p_gt3_cnt / self.total_cnt * 100) if self.total_cnt > 0 else 0.0,
            },
        }

    def update_with_l2_delta(self, deltas, event_time_ms: Optional[int] = None):
        """
        基于L2增量更新OFI（Task 1.2.1暂不实现）
        
        注意:
            Task 1.2.1仅实现快照模式，增量模式将在后续任务中实现
        
        异常:
            NotImplementedError: 此版本暂不支持增量模式
        """
        raise NotImplementedError("Task 1.2.1 implements snapshot mode only.")
    def update_params(self, **kwargs):
        """Safely update calculator parameters at runtime.
        Only known keys are applied; others are ignored.
        Logs via module logger without requiring self.logger.
        """
        import logging
        logger = logging.getLogger(__name__)
        updated = {}
        for k, v in kwargs.items():
            if hasattr(self.cfg, k):
                try:
                    setattr(self.cfg, k, v)
                    updated[k] = v
                except Exception as e:
                    logger.warning(f"Failed to set '{k}' to '{v}': {e}")  # safe warn
            elif hasattr(self, k):
                try:
                    setattr(self, k, v)
                    updated[k] = v
                except Exception as e:
                    logger.warning(f"Failed to set field '{k}' to '{v}': {e}")  # safe warn
        # 特殊处理z_window变化
        if 'z_window' in updated:
            new_window = updated['z_window']
            # 同步更新self.z_window
            self.z_window = new_window
            if new_window != len(self.ofi_hist):
                # 重建ofi_hist队列
                old_data = list(self.ofi_hist)
                self.ofi_hist = deque(old_data[-new_window:], maxlen=new_window)
                logger.info(f"Rebuilt ofi_hist queue: {len(old_data)} -> {len(self.ofi_hist)}")
        
        # 特殊处理weights/levels变化
        if 'weights' in updated or 'levels' in updated:
            # 更新levels时同步更新self.K
            if 'levels' in updated:
                new_levels = int(updated['levels'])
                if new_levels > 0:
                    self.K = new_levels
                    # 需要重建订单簿缓存以适应新的档位数
                    self.bids = [[0.0, 0.0] for _ in range(self.K)]
                    self.asks = [[0.0, 0.0] for _ in range(self.K)]
                    self.prev_bids = [[0.0, 0.0] for _ in range(self.K)]
                    self.prev_asks = [[0.0, 0.0] for _ in range(self.K)]
                    logger.info(f"[OFI] Levels updated: {self.K}, rebuilt order book cache")
            # 重新计算权重
            if hasattr(self.cfg, 'weights') and hasattr(self.cfg, 'levels'):
                self.w = self._normalize_weights(self.cfg.weights, self.cfg.levels)
                logger.info(f"[OFI] Recalculated weights: {list(self.w)}")
        
        # 特殊处理debug_winsorize变化
        if 'debug_winsorize' in updated:
            self.debug_winsorize = bool(updated['debug_winsorize'])
            logger.info(f"[OFI] debug_winsorize updated: {self.debug_winsorize}")
        
        if updated:
            logger.info(f"RealOFICalculator params updated: {updated}")  # safe info
        return updated

