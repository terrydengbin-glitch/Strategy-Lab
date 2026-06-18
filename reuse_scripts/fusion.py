# -*- coding: utf-8 -*-
"""
Fusion (OFI+CVD融合) 计算引擎

封装 OFI_CVD_Fusion，提供简化的纯计算接口。
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from .ofi_cvd_fusion import OFI_CVD_Fusion, OFICVDFusionConfig


@dataclass
class FusionParams:
    """
    Fusion计算参数
    
    注意：
    - calibration_k 已移除，因为底层OFICVDFusionConfig不支持此参数
    """
    w_ofi: float = 0.6
    w_cvd: float = 0.4
    consistency_min: float = 0.3
    consistency_strong_min: float = 0.5


class FusionEngine:
    """
    Fusion计算引擎
    
    纯计算接口，不依赖I/O操作。
    
    使用示例:
        >>> params = FusionParams(w_ofi=0.6, w_cvd=0.4)
        >>> engine = FusionEngine(params)
        >>> result = engine.fuse(
        ...     ts_ms=1234567890,
        ...     z_ofi=1.5,
        ...     z_cvd=2.0
        ... )
        >>> print(f"Score={result['score']:.4f}, Consistency={result['consistency']:.4f}")
    """
    
    def __init__(self, params: Optional[FusionParams] = None):
        """
        初始化Fusion引擎
        
        Args:
            params: Fusion参数，默认使用FusionParams默认值
        """
        if params is None:
            params = FusionParams()
        
        # 转换为OFICVDFusionConfig
        config = OFICVDFusionConfig(
            w_ofi=params.w_ofi,
            w_cvd=params.w_cvd,
            min_consistency=params.consistency_min,
            strong_min_consistency=params.consistency_strong_min
        )
        
        # 创建底层融合器
        self._fusion = OFI_CVD_Fusion(cfg=config)
    
    def fuse(
        self,
        ts_ms: int,
        z_ofi: Optional[float],
        z_cvd: Optional[float],
        lag_sec: float = 0.0,
    ) -> Dict[str, Any]:
        """
        融合OFI和CVD的Z-score
        
        Args:
            ts_ms: 时间戳（毫秒）
            z_ofi: OFI Z-score（可为None）
            z_cvd: CVD Z-score（可为None）
            lag_sec: OFI/CVD时间差（秒），默认0.0。在实时pipeline中可传入真实lag值
        
        Returns:
            包含以下字段的字典:
            - score: 融合分数
            - consistency: 一致性分数 [0, 1]
            - signal: 信号类型 ("neutral", "buy", "sell", "strong_buy", "strong_sell")
            - warmup: 是否在warmup期
            - components: {"ofi": float, "cvd": float} 组件贡献
        """
        # 处理None值
        if z_ofi is None:
            z_ofi = 0.0
        if z_cvd is None:
            z_cvd = 0.0
        
        # 转换为秒级时间戳
        ts_sec = ts_ms / 1000.0
        
        result = self._fusion.update(
            z_ofi=z_ofi,
            z_cvd=z_cvd,
            ts=ts_sec,
            lag_sec=lag_sec  # 使用传入的lag_sec参数
        )
        
        # 提取并标准化返回结果
        return {
            'score': result.get('fusion_score', 0.0),
            'consistency': result.get('consistency', 0.0),
            'signal': result.get('signal', 'neutral'),
            'warmup': result.get('warmup', False),
            'components': result.get('components', {'ofi': 0.0, 'cvd': 0.0}),
        }
    
    def reset(self):
        """
        重置融合器状态
        
        重置内部状态，包括_last_signal、_streak、_prev_raw_signal等。
        适用于需要清空融合器历史状态的场景。
        """
        if hasattr(self._fusion, "reset"):
            self._fusion.reset()
    
    def get_state(self) -> Dict[str, Any]:
        """获取当前状态（用于调试）"""
        stats = getattr(self._fusion, '_stats', {})
        return {
            # 注意：_is_warmup是内部bool状态字段，不是方法
            'warmup': getattr(self._fusion, '_is_warmup', True),
            'stats': stats.copy() if stats else {},
        }

