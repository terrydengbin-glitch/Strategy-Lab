# -*- coding: utf-8 -*-
"""
CVD (Cumulative Volume Delta) 计算引擎

封装 RealCVDCalculator，提供简化的纯计算接口。
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .real_cvd_calculator import RealCVDCalculator, CVDConfig


@dataclass
class CVDParams:
    """
    CVD计算参数
    
    注意：
    - use_tick_rule: 默认False，因为update_with_trade已经通过side参数明确指定了方向。
      如果需要在side=None时使用tick rule，请设置use_tick_rule=True。
    """
    z_window: int = 200
    ema_alpha: float = 0.3
    use_tick_rule: bool = False  # 默认False，因为side参数已明确方向
    warmup_min: float = 0.5


class CVDEngine:
    """
    CVD计算引擎
    
    纯计算接口，不依赖I/O操作。
    
    使用示例:
        >>> params = CVDParams(z_window=200, use_tick_rule=True)
        >>> engine = CVDEngine("ETHUSDT", params)
        >>> result = engine.update_with_trade(
        ...     ts_ms=1234567890,
        ...     price=3245.5,
        ...     qty=10.5,
        ...     side="buy"
        ... )
        >>> print(f"CVD={result['cvd']:.4f}, Z-score={result.get('z_cvd')}")
    """
    
    def __init__(self, symbol: str, params: Optional[CVDParams] = None):
        """
        初始化CVD引擎
        
        Args:
            symbol: 交易对符号（如"ETHUSDT"）
            params: CVD参数，默认使用CVDParams默认值
        """
        if params is None:
            params = CVDParams()
        
        # 转换为CVDConfig
        config = CVDConfig(
            z_window=params.z_window,
            ema_alpha=params.ema_alpha,
            use_tick_rule=params.use_tick_rule,
            warmup_min=params.warmup_min
        )
        
        # 创建底层计算器
        self._calculator = RealCVDCalculator(symbol, cfg=config)
        self.symbol = symbol
    
    def update_with_trade(
        self,
        ts_ms: int,
        price: float,
        qty: float,
        side: Optional[str] = None,  # "buy", "sell", 或 None（使用tick rule）
    ) -> Dict[str, Any]:
        """
        基于成交更新CVD
        
        Args:
            ts_ms: 时间戳（毫秒）
            price: 成交价格
            qty: 成交数量
            side: 买卖方向，"buy"、"sell" 或 None（当为None时，如果use_tick_rule=True则使用tick rule判定）
        
        Returns:
            包含以下字段的字典:
            - cvd: CVD原始值
            - z_cvd: CVD Z-score（warmup期间可能为None）
            - ema_cvd: EMA平滑后的CVD
            - warmup: 是否在warmup期
            - std_zero: 标准差是否为0
            - bad_points: 坏数据点计数
            - symbol: 交易对符号（便于后续Feature构建）
        """
        # 转换side为is_buy
        is_buy = None
        if side == "buy":
            is_buy = True
        elif side == "sell":
            is_buy = False
        elif side is None:
            is_buy = None  # 让底层使用tick rule
        else:
            raise ValueError(f"Invalid side: {side}, must be 'buy', 'sell', or None")
        
        result = self._calculator.update_with_trade(
            price=price,
            qty=qty,
            is_buy=is_buy,
            event_time_ms=ts_ms
        )
        
        # 提取并标准化返回结果
        meta = result.get('meta', {})
        return {
            'cvd': result.get('cvd', 0.0),
            'z_cvd': result.get('z_cvd'),
            'ema_cvd': result.get('ema_cvd', 0.0),
            'warmup': meta.get('warmup', False),
            'std_zero': meta.get('std_zero', False),
            'bad_points': meta.get('bad_points', 0),
            'symbol': self.symbol,  # 添加symbol字段，方便直接拼Feature行
        }
    
    def reset(self):
        """重置计算器状态"""
        self._calculator.reset()
    
    def get_state(self) -> Dict[str, Any]:
        """获取当前状态（用于调试）"""
        return {
            'symbol': self.symbol,
            'warmup': getattr(self._calculator, '_last_is_warmup', True),  # 修复：使用_last_is_warmup字段
            'bad_points': self._calculator.bad_points,
        }

