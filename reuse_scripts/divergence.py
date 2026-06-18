# -*- coding: utf-8 -*-
"""
Divergence (背离检测) 计算引擎

封装 DivergenceDetector，提供简化的纯计算接口。
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from .ofi_cvd_divergence import DivergenceDetector, DivergenceConfig


@dataclass
class DivergenceParams:
    """Divergence计算参数"""
    swing_L_confirmed: int = 12
    swing_L_fast: int = 5
    mode: str = "confirmed"  # "confirmed" or "fast"
    max_pivots: int = 100
    warmup_min: int = 10
    cooldown_secs: float = 60.0


class DivergenceEngine:
    """
    Divergence计算引擎
    
    纯计算接口，不依赖I/O操作。
    
    使用示例:
        >>> params = DivergenceParams(swing_L_confirmed=12)
        >>> engine = DivergenceEngine(params)
        >>> events = engine.update(
        ...     ts_ms=1234567890,
        ...     price=3245.5,
        ...     fusion_score=1.5
        ... )
        >>> for event in events:
        ...     print(f"Divergence: {event['type']}")
    """
    
    def __init__(self, params: Optional[DivergenceParams] = None):
        """
        初始化Divergence引擎
        
        Args:
            params: Divergence参数，默认使用DivergenceParams默认值
        """
        if params is None:
            params = DivergenceParams()
        
        # 转换为DivergenceConfig
        config = DivergenceConfig(
            swing_L=params.swing_L_confirmed,  # 向后兼容
            swing_L_confirmed=params.swing_L_confirmed,
            swing_L_fast=params.swing_L_fast,
            mode=params.mode,
            max_pivots=params.max_pivots,
            warmup_min=params.warmup_min,
            cooldown_secs=params.cooldown_secs
        )
        
        # 创建底层检测器
        self._detector = DivergenceDetector(config=config)
    
    def update(
        self,
        ts_ms: int,
        price: float,
        z_ofi: Optional[float] = None,
        z_cvd: Optional[float] = None,
        fusion_score: Optional[float] = None,
        consistency: Optional[float] = None,
        warmup: bool = False,
        lag_sec: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """
        更新背离检测器
        
        Args:
            ts_ms: 时间戳（毫秒）
            price: 价格
            z_ofi: OFI Z-score（可选，用于价格vs OFI背离检测）
            z_cvd: CVD Z-score（可选，用于价格vs CVD背离检测）
            fusion_score: 融合分数（可选，用于价格vs Fusion背离检测）
            consistency: 一致性分数（可选）
            warmup: 是否在warmup期
            lag_sec: OFI/CVD时间差（秒）
        
        Returns:
            背离事件列表（0~N个事件），每个事件包含:
            - ts: 时间戳（毫秒）
            - price: 价格
            - type: 背离类型（人类可读）
            - divergence_type: 背离类型（机器友好）
            - strength: 强度评分
            - channel: 检测通道（"ofi", "cvd", "fusion"）
            - score: 融合分数（如果使用fusion通道）
            - raw: 原始事件完整字典（包含pivots等详细信息，便于后续Divergence Guard/P-ENGINE使用）
        """
        # 转换为秒级时间戳
        ts_sec = ts_ms / 1000.0
        
        # 处理None值：如果未提供，使用0.0（让底层处理）
        z_ofi_val = z_ofi if z_ofi is not None else 0.0
        z_cvd_val = z_cvd if z_cvd is not None else 0.0
        
        # 调用底层检测器
        result = self._detector.update(
            ts=ts_sec,
            price=price,
            z_ofi=z_ofi_val,
            z_cvd=z_cvd_val,
            fusion_score=fusion_score,
            consistency=consistency,
            warmup=warmup,
            lag_sec=lag_sec
        )
        
        # 处理返回结果，保留更多字段
        events = []
        if result is not None:
            # 如果返回单个事件，转换为列表
            if isinstance(result, dict):
                events.append({
                    'ts': ts_ms,
                    'price': price,
                    'type': result.get('type', 'unknown'),
                    'divergence_type': result.get('divergence_type', result.get('type', 'unknown')),
                    'strength': result.get('strength', result.get('score', 0.0)),
                    'channel': result.get('channel', 'fusion' if fusion_score is not None else 'unknown'),
                    'score': fusion_score if fusion_score is not None else 0.0,
                    'raw': result,  # 保留原始事件完整信息
                })
            elif isinstance(result, list):
                for event in result:
                    events.append({
                        'ts': ts_ms,
                        'price': price,
                        'type': event.get('type', 'unknown'),
                        'divergence_type': event.get('divergence_type', event.get('type', 'unknown')),
                        'strength': event.get('strength', event.get('score', 0.0)),
                        'channel': event.get('channel', 'fusion' if fusion_score is not None else 'unknown'),
                        'score': fusion_score if fusion_score is not None else 0.0,
                        'raw': event,  # 保留原始事件完整信息
                    })
        
        return events
    
    def reset(self):
        """重置检测器状态"""
        self._detector.reset()  # 使用公开方法
    
    def get_state(self) -> Dict[str, Any]:
        """获取当前状态（用于调试）"""
        return {
            'sample_count': getattr(self._detector, '_sample_count', 0),
            'stats': self._detector.get_stats(),  # 使用公开方法
        }

