# -*- coding: utf-8 -*-
"""
OFI (Order Flow Imbalance) 计算引擎

封装 RealOFICalculator，提供简化的纯计算接口。
"""

from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from .real_ofi_calculator import RealOFICalculator, OFIConfig


@dataclass
class OFIParams:
    """OFI计算参数"""
    levels: int = 5
    z_window: int = 75
    ema_alpha: float = 0.3
    z_clip: float = 3.0
    reset_on_gap_ms: int = 2000
    reset_on_session_change: bool = True
    per_symbol_window: bool = True
    weights: Optional[List[float]] = None


class OFIEngine:
    """
    OFI计算引擎
    
    纯计算接口，不依赖I/O操作。
    
    使用示例:
        >>> params = OFIParams(levels=5, z_window=75)
        >>> engine = OFIEngine("ETHUSDT", params)
        >>> result = engine.update_with_snapshot(
        ...     ts_ms=1234567890,
        ...     bids=[(3245.5, 10.5), (3245.4, 8.3)],
        ...     asks=[(3245.6, 11.2), (3245.7, 9.5)]
        ... )
        >>> print(f"OFI={result['ofi']:.4f}, Z-score={result.get('z_ofi')}")
    """
    
    def __init__(self, symbol: str, params: Optional[OFIParams] = None):
        """
        初始化OFI引擎
        
        Args:
            symbol: 交易对符号（如"ETHUSDT"）
            params: OFI参数，默认使用OFIParams默认值
        """
        if params is None:
            params = OFIParams()
        
        # 转换为OFIConfig
        config = OFIConfig(
            levels=params.levels,
            weights=params.weights,
            z_window=params.z_window,
            ema_alpha=params.ema_alpha,
            z_clip=params.z_clip,
            reset_on_gap_ms=params.reset_on_gap_ms,
            reset_on_session_change=params.reset_on_session_change,
            per_symbol_window=params.per_symbol_window
        )
        
        # 创建底层计算器
        self._calculator = RealOFICalculator(symbol, cfg=config)
        self.symbol = symbol
    
    def update_with_snapshot(
        self,
        ts_ms: int,
        bids: List[Tuple[float, float]],
        asks: List[Tuple[float, float]],
    ) -> Dict[str, Any]:
        """
        基于订单簿快照更新OFI
        
        Args:
            ts_ms: 时间戳（毫秒）
            bids: 买单列表 [(价格, 数量), ...] 按价格降序
            asks: 卖单列表 [(价格, 数量), ...] 按价格升序
        
        Returns:
            包含以下字段的字典:
            - ofi: OFI原始值
            - z_ofi: OFI Z-score（warmup期间可能为None）
            - ema_ofi: EMA平滑后的OFI
            - warmup: 是否在warmup期
            - std_zero: 标准差是否为0
            - session_reset: 是否发生会话重置
            - bad_points: 坏数据点计数
            - symbol: 交易对符号（便于后续Feature构建）
        """
        result = self._calculator.update_with_snapshot(
            bids=bids,
            asks=asks,
            event_time_ms=ts_ms,
            current_symbol=self.symbol
        )
        
        # 提取并标准化返回结果
        meta = result.get('meta', {})
        return {
            'ofi': result.get('ofi', 0.0),
            'z_ofi': result.get('z_ofi'),  # 统一命名：z_ofi（与Feature行一致）
            'ema_ofi': result.get('ema_ofi', 0.0),
            'warmup': meta.get('warmup', False),
            'std_zero': meta.get('std_zero', False),
            'session_reset': meta.get('session_reset', False),
            'bad_points': meta.get('bad_points', 0),
            'symbol': self.symbol,  # 添加symbol字段，方便直接拼Feature行
        }
    
    def reset(self):
        """
        重置计算器状态（会话重置）
        
        清空历史数据，但保留当前盘口状态。
        适用于数据间隔重置场景（gap reset）。
        """
        self._calculator._reset_session()
    
    def full_reset(self):
        """
        完全重置计算器状态
        
        清空所有状态，包括历史数据和盘口数组。
        适用于需要彻底重置的场景。
        """
        self._calculator.reset()
    
    def get_state(self) -> Dict[str, Any]:
        """获取当前状态（用于调试）"""
        return {
            'symbol': self.symbol,
            'warmup': len(self._calculator.ofi_hist) < self._calculator.z_window,
            'history_size': len(self._calculator.ofi_hist),
            'bad_points': self._calculator.bad_points,
        }

