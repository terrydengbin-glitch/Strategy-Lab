"""
OFI+CVD融合指标模块

实现订单流不平衡(OFI)和累积成交量差值(CVD)的融合信号生成，
包含时间对齐、降级机制、去噪三件套等核心功能。

Author: V13 OFI+CVD AI Trading System
Date: 2025-10-19
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import math
import time
import logging
from enum import Enum


class SignalType(Enum):
    """信号类型枚举"""
    NEUTRAL = "neutral"
    BUY = "buy"
    STRONG_BUY = "strong_buy"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


@dataclass
class OFICVDFusionConfig:
    """OFI+CVD融合配置"""
    # 权重配置
    w_ofi: float = 0.6
    w_cvd: float = 0.4
    
    # 信号阈值 - 包B+配置（激进调整，目标 6-9%）
    fuse_buy: float = 0.95         # 1.0 → 0.95 (包B: 温和降低买入门槛)
    fuse_strong_buy: float = 1.70  # 1.8 → 1.70 (包B: 温和降低强买入门槛)
    fuse_sell: float = -0.95       # -1.0 → -0.95 (包B: 温和降低卖出门槛)
    fuse_strong_sell: float = -1.70 # -1.8 → -1.70 (包B: 温和降低强卖出门槛)
    
    # 一致性阈值 - 包B+配置（更激进降低一致性要求）
    min_consistency: float = 0.12  # 0.15 → 0.12 (包B: 温和降低一致性要求)
    strong_min_consistency: float = 0.45  # 0.5 → 0.45 (包B: 温和降低强一致性要求)
    
    # 数据处理 - 包B配置
    z_clip: float = 4.0            # 5.0 → 4.0 (放宽Z-score裁剪)
    max_lag: float = 3.0          # 0.25 → 3.0 (与 lag_cap_sec 保持一致，避免不必要的降级)
    
    # 去噪参数 - 包B+配置（继续减少冷却时间）
    hysteresis_exit: float = 0.6  # 0.8 → 0.6 (进一步减小迟滞)
    cooldown_secs: float = 0.3    # 保持 0.3 (propsB+ 冷却时间)
    min_consecutive: int = 1      # 保持1 (最低持续门槛)
    
    # 暖启动 - 包B+配置
    min_warmup_samples: int = 10   # 保持 10 (包B+ 暖启动样本数)
    
    # 高级机制配置（突破冷却限制）
    rearm_on_flip: bool = True  # 方向翻转即重臂
    flip_rearm_margin: float = 0.05  # 重臂余量δ=5%
    
    adaptive_cooldown_enabled: bool = True  # 自适应冷却
    adaptive_cooldown_k: float = 0.6  # 收缩系数
    adaptive_cooldown_min_secs: float = 0.12  # 最小冷却时间
    
    burst_coalesce_ms: float = 120.0  # 微型突发合并窗口（毫秒）


class OFI_CVD_Fusion:
    """
    OFI+CVD融合信号生成器
    
    核心功能:
    1. 权重融合: w_ofi * z_ofi + w_cvd * z_cvd
    2. 时间对齐: 处理OFI/CVD不同步问题
    3. 降级机制: 数据缺失时降级为单因子
    4. 去噪三件套: 迟滞/冷却/最小持续
    5. 暖启动保护: 数据不足时返回neutral
    """
    
    def __init__(self, cfg: OFICVDFusionConfig = None, config_loader=None, verbose: bool = False,
                 runtime_cfg: Optional[Dict[str, Any]] = None):
        """
        初始化融合器
        
        Args:
            cfg: 融合配置，默认使用标准配置
            config_loader: 配置加载器实例（兼容旧接口，库式调用时不应使用）
            verbose: 是否启用详细日志输出
            runtime_cfg: 运行时配置字典，库式调用时使用（优先于config_loader）
        """
        self._config_loader = config_loader  # 保存配置来源，便于可观测
        self._verbose = verbose  # 日志开关
        self._logger = logging.getLogger(__name__)  # 模块级logger
        
        # TASK-G3-REGIME-2x2-ACTIVATION: 场景级 consistency_min 支持
        self._scenario_min_consistency = None  # 场景级 min_consistency（优先级最高）
        self._scenario_strong_min_consistency = None  # 场景级 strong_min_consistency（优先级最高）
        
        # [P1 FIX] Consistency日志降噪：lag_exceeded时状态变化+周期汇总
        self._lag_exceeded_state: Dict[str, bool] = {}  # {symbol: 是否处于lag_exceeded状态}
        self._lag_exceeded_last_log_ts: Dict[str, float] = {}  # {symbol: 上次记录状态变化的时间}
        self._lag_exceeded_summary_last_ts: float = 0.0  # 上次汇总统计的时间
        self._lag_exceeded_summary_interval_sec: float = 60.0  # 汇总统计间隔（秒）
        self._lag_exceeded_stats: Dict[str, Dict[str, int]] = {}  # {symbol: {"count": ..., "last_ts": ...}}
        
        # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 降级模式恢复机制
        self._last_degraded_state: Dict[str, bool] = {}  # {symbol: 上次的降级状态}
        self._degraded_start_ts: Dict[str, float] = {}  # {symbol: 进入降级模式的时间戳}
        self._normal_lag_count: Dict[str, int] = {}  # {symbol: 连续正常延迟的次数}
        
        # 优先使用运行时配置字典（库式调用）
        if runtime_cfg is not None:
            fusion_cfg = runtime_cfg.get('fusion', {}) if isinstance(runtime_cfg, dict) else {}
            # 从运行时配置构建OFICVDFusionConfig对象
            weights = fusion_cfg.get('weights', {})
            thresholds = fusion_cfg.get('thresholds', {})
            consistency = fusion_cfg.get('consistency', {})
            data_processing = fusion_cfg.get('data_processing', {})
            denoising = fusion_cfg.get('denoising', {})
            
            default = OFICVDFusionConfig()
            
            # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 读取降级模式恢复配置
            degraded_recovery_cfg = fusion_cfg.get('degraded_recovery', {})
            self._degraded_recovery_enabled = degraded_recovery_cfg.get('enabled', True)
            self._degraded_recovery_replay_like_exit = degraded_recovery_cfg.get('replay_like_exit', True)
            self._degraded_recovery_normal_lag_threshold = degraded_recovery_cfg.get('normal_lag_threshold', 5)
            
            self.cfg = OFICVDFusionConfig(
                w_ofi=weights.get('w_ofi', default.w_ofi),
                w_cvd=weights.get('w_cvd', default.w_cvd),
                fuse_buy=thresholds.get('fuse_buy', default.fuse_buy),
                fuse_strong_buy=thresholds.get('fuse_strong_buy', default.fuse_strong_buy),
                fuse_sell=thresholds.get('fuse_sell', default.fuse_sell),
                fuse_strong_sell=thresholds.get('fuse_strong_sell', default.fuse_strong_sell),
                min_consistency=consistency.get('min_consistency', default.min_consistency),
                strong_min_consistency=consistency.get('strong_min_consistency', default.strong_min_consistency),
                z_clip=data_processing.get('z_clip', default.z_clip) if data_processing else default.z_clip,
                max_lag=data_processing.get('max_lag', default.max_lag) if data_processing else default.max_lag,
                min_warmup_samples=data_processing.get('warmup_samples', default.min_warmup_samples) if data_processing else default.min_warmup_samples,
                hysteresis_exit=denoising.get('hysteresis_exit', default.hysteresis_exit) if denoising else default.hysteresis_exit,
                cooldown_secs=denoising.get('cooldown_secs', default.cooldown_secs) if denoising else default.cooldown_secs,
                min_consecutive=denoising.get('min_duration', default.min_consecutive) if denoising else default.min_consecutive
            )
        elif config_loader:
            # 从统一配置系统加载参数（兼容旧接口）
            self.cfg = self._load_from_config_loader(config_loader)
            # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 从配置加载器读取降级模式恢复配置
            try:
                degraded_recovery_cfg = config_loader.get('feature_engine.fusion.degraded_recovery', {})
                self._degraded_recovery_enabled = degraded_recovery_cfg.get('enabled', True)
                self._degraded_recovery_replay_like_exit = degraded_recovery_cfg.get('replay_like_exit', True)
                self._degraded_recovery_normal_lag_threshold = degraded_recovery_cfg.get('normal_lag_threshold', 5)
            except Exception:
                # 如果配置不存在，使用默认值
                self._degraded_recovery_enabled = True
                self._degraded_recovery_replay_like_exit = True
                self._degraded_recovery_normal_lag_threshold = 5
        else:
            self.cfg = cfg or OFICVDFusionConfig()
            # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 使用默认配置
            self._degraded_recovery_enabled = True
            self._degraded_recovery_replay_like_exit = True
            self._degraded_recovery_normal_lag_threshold = 5
        
        # 权重归一化
        total_weight = self.cfg.w_ofi + self.cfg.w_cvd
        if total_weight <= 0:
            raise ValueError("权重和必须大于0")
        self.w_ofi = self.cfg.w_ofi / total_weight
        self.w_cvd = self.cfg.w_cvd / total_weight
        
        # 状态管理
        self._last_signal = SignalType.NEUTRAL
        self._last_emit_ts: Optional[float] = None
        self._streak = 0
        self._prev_raw_signal: Optional[SignalType] = None  # 记录"原始判定"用于连击门槛
        self._warmup_count = 0
        self._is_warmup = True
        # default regime for consistency thresholds
        self._current_regime = 'normal'
        
        # 运行时场景一致性配置缓存
        self._regime_consistency = None
        
        # 高级机制状态（突破冷却限制）
        self._last_signal_direction = 0  # 1=buy, -1=sell, 0=neutral
        self._burst_window_candidates = []  # 突发合并候选池
        self._burst_window_start = None  # 突发窗口开始时间
        
        # 统计信息
        self._stats = {
            'total_updates': 0,
            'downgrades': 0,
            'warmup_returns': 0,
            'invalid_inputs': 0,
            'lag_exceeded': 0,
            'cooldown_blocks': 0,
            'min_duration_blocks': 0,
            'flip_rearm': 0,
            'adaptive_cooldown_used': 0,
            'burst_coalesced': 0
        }
    
    def _load_from_config_loader(self, config_loader) -> OFICVDFusionConfig:
        """
        从统一配置系统加载融合指标参数
        
        Args:
            config_loader: 配置加载器实例
            
        Returns:
            融合指标配置对象
        """
        try:
            # 获取融合指标配置
            fusion_config = config_loader.get('fusion_metrics', {})
            
            # 提取权重配置
            weights = fusion_config.get('weights', {})
            w_ofi = weights.get('w_ofi', 0.6)
            w_cvd = weights.get('w_cvd', 0.4)
            
            # 提取阈值配置
            thresholds = fusion_config.get('thresholds', {})
            fuse_buy = thresholds.get('fuse_buy', 1.5)
            fuse_strong_buy = thresholds.get('fuse_strong_buy', 2.5)
            fuse_sell = thresholds.get('fuse_sell', -1.5)
            fuse_strong_sell = thresholds.get('fuse_strong_sell', -2.5)
            
            # 提取一致性配置（支持分场景）
            consistency = fusion_config.get('consistency', {})
            regime_consistency = consistency.get('regime_consistency', {})
            
            # 获取当前regime（从核心算法传递）
            current_regime = getattr(self, '_current_regime', 'normal')
            regime_config = regime_consistency.get(current_regime, {})
            
            if regime_config:
                # 使用分场景一致性阈值
                min_consistency = regime_config.get('min_consistency', consistency.get('min_consistency', 0.15))
                strong_min_consistency = regime_config.get('strong_min_consistency', consistency.get('strong_min_consistency', 0.4))
            else:
                # 使用基础一致性阈值
                min_consistency = consistency.get('min_consistency', 0.15)
                strong_min_consistency = consistency.get('strong_min_consistency', 0.4)
            
            # TASK-G3-REGIME-2x2-ACTIVATION: 应用场景级 consistency_min（优先级最高）
            # 优先级：场景级 > Regime 级 > Global 默认值
            scenario_min = getattr(self, '_scenario_min_consistency', None)
            scenario_strong = getattr(self, '_scenario_strong_min_consistency', None)
            
            if scenario_min is not None:
                min_consistency = scenario_min
                self._logger.debug(f"[Fusion] Using scenario min_consistency: {min_consistency}")
            if scenario_strong is not None:
                strong_min_consistency = scenario_strong
                self._logger.debug(f"[Fusion] Using scenario strong_min_consistency: {strong_min_consistency}")
            
            # 提取数据处理配置
            data_processing = fusion_config.get('data_processing', {})
            z_clip = data_processing.get('z_clip', 5.0)
            max_lag = data_processing.get('max_lag', 3.0)  # 与 lag_cap_sec 保持一致，避免不必要的降级
            warmup_samples = data_processing.get('warmup_samples', 30)
            
            # 提取去噪配置
            denoising = fusion_config.get('denoising', {})
            hysteresis_exit = denoising.get('hysteresis_exit', 1.2)
            cooldown_secs = denoising.get('cooldown_secs', 1.0)
            min_duration = denoising.get('min_duration', 2)
            
            # 创建配置对象
            return OFICVDFusionConfig(
                w_ofi=w_ofi,
                w_cvd=w_cvd,
                fuse_buy=fuse_buy,
                fuse_strong_buy=fuse_strong_buy,
                fuse_sell=fuse_sell,
                fuse_strong_sell=fuse_strong_sell,
                min_consistency=min_consistency,
                strong_min_consistency=strong_min_consistency,
                z_clip=z_clip,
                max_lag=max_lag,
                hysteresis_exit=hysteresis_exit,
                cooldown_secs=cooldown_secs,
                min_consecutive=min_duration,
                min_warmup_samples=warmup_samples
            )
            
        except Exception as e:
            # 如果配置加载失败，使用默认配置并记录警告
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to load fusion metrics config from config_loader: {e}. Using default config.")
            return OFICVDFusionConfig()
    
    def _consistency(self, z_ofi: float, z_cvd: float) -> float:
        """
        计算信号一致性
        
        Args:
            z_ofi: OFI Z-score
            z_cvd: CVD Z-score
            
        Returns:
            一致性得分 (0-1之间，更高=更一致)
        """
        # 使用极小epsilon避免浮点/裁剪造成的0被误判
        eps = 1e-9
        if abs(z_ofi) < eps or abs(z_cvd) < eps:
            return 0.0
        
        # 方向一致性检查
        if math.copysign(1, z_ofi) != math.copysign(1, z_cvd):
            return 0.0
        
        # 强度一致性 (较小值/较大值)
        abs_ofi, abs_cvd = abs(z_ofi), abs(z_cvd)
        consistency = min(abs_ofi, abs_cvd) / max(abs_ofi, abs_cvd)
        
        # [诊断日志] 当 consistency=1.0 时记录详细信息（帮助判断是否正常）
        # 注意：这个方法只计算 value，不涉及 mode/effective
        if abs(consistency - 1.0) < 1e-6:  # 浮点数比较，允许小误差
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(
                f"[CONSISTENCY_DIAG] consistency_value=1.000 (reason=perfect_match): "
                f"abs_ofi={abs_ofi:.6f}, abs_cvd={abs_cvd:.6f}, "
                f"diff={abs(abs_ofi - abs_cvd):.9f}"
            )
        
        return consistency
    
    def _consistency_reason(self, z_ofi: float, z_cvd: float) -> str:
        """
        给 consistency=0.0 提供可解释原因码（最小侵入，不改变原公式）
        
        Args:
            z_ofi: OFI Z-score
            z_cvd: CVD Z-score
            
        Returns:
            原因码字符串：'too_small', 'opposite_sign', 或 'zero_consistency'
        """
        eps = 1e-9
        if abs(z_ofi) < eps or abs(z_cvd) < eps:
            return "too_small"
        if math.copysign(1, z_ofi) != math.copysign(1, z_cvd):
            return "opposite_sign"
        return "zero_consistency"
    
    def _clip_z_score(self, z: float) -> float:
        """裁剪Z-score到合理范围"""
        return max(-self.cfg.z_clip, min(self.cfg.z_clip, z))
    
    def _check_warmup(self) -> bool:
        """检查是否还在暖启动期"""
        if self._warmup_count < self.cfg.min_warmup_samples:
            self._warmup_count += 1
            return True
        self._is_warmup = False
        return False
    
    def _apply_denoising(self, signal: SignalType, fusion_score: float, 
                        ts: float, consistency: float = 0.0) -> tuple[SignalType, list]:
        """
        应用去噪三件套 + 高级机制（方向翻转重臂、自适应冷却、突发合并）
        
        Args:
            signal: 原始信号
            fusion_score: 融合得分
            ts: 时间戳
            consistency: 一致性得分
            
        Returns:
            (去噪后的信号, 去噪原因列表)
        """
        denoising_reasons = []
        original_signal = signal
        
        # 获取信号方向
        current_direction = 0  # 1=buy, -1=sell, 0=neutral
        if signal in [SignalType.BUY, SignalType.STRONG_BUY]:
            current_direction = 1
        elif signal in [SignalType.SELL, SignalType.STRONG_SELL]:
            current_direction = -1
        
        # 连击计数：基于"原始判定信号"，而非上次已发出的信号
        if original_signal is not SignalType.NEUTRAL:
            if self._prev_raw_signal == original_signal:
                self._streak += 1
            else:
                self._streak = 1
        else:
            self._streak = 0
        self._prev_raw_signal = original_signal
        
        # 1. 冷却时间检查 + 高级机制
        cooldown_passed = True
        effective_cooldown = self.cfg.cooldown_secs
        
        if self._last_emit_ts:
            elapsed = ts - self._last_emit_ts
            
            # 机制1: 自适应冷却 - 根据信号强度动态调整
            if self.cfg.adaptive_cooldown_enabled and elapsed < self.cfg.cooldown_secs:
                # 计算超阈强度
                strength = 0.0
                if abs(fusion_score) >= abs(self.cfg.fuse_buy):
                    strength = min(1.0, (abs(fusion_score) - abs(self.cfg.fuse_buy)) / 
                                  (abs(self.cfg.fuse_strong_buy) - abs(self.cfg.fuse_buy)))
                
                # 有效冷却 = max(最小冷却, 基础冷却 * (1 - k * 强度))
                effective_cooldown = max(
                    self.cfg.adaptive_cooldown_min_secs,
                    self.cfg.cooldown_secs * (1 - self.cfg.adaptive_cooldown_k * strength)
                )
                if effective_cooldown < self.cfg.cooldown_secs:
                    self._stats['adaptive_cooldown_used'] += 1
            
            # 机制2: 方向翻转即重臂 - 方向反转时提前解锁
            rearm = False
            if self.cfg.rearm_on_flip and current_direction != 0:
                if self._last_signal_direction != 0 and current_direction != self._last_signal_direction:
                    # 方向翻转，检查是否超阈足够
                    threshold = abs(self.cfg.fuse_buy) * (1 + self.cfg.flip_rearm_margin)
                    if abs(fusion_score) >= threshold:
                        rearm = True
                        denoising_reasons.append("flip_rearm")
                        self._stats['flip_rearm'] += 1
            
            # 判断是否通过冷却
            if not rearm and elapsed < effective_cooldown:
                cooldown_passed = False
            
            # 冷却检查结果
            if not cooldown_passed and signal != SignalType.NEUTRAL:
                denoising_reasons.append("cooldown")
                self._stats['cooldown_blocks'] += 1
                # 记录到突发合并候选池（如果启用）
                if self.cfg.burst_coalesce_ms > 0:
                    if (self._burst_window_start is None or 
                        (ts - self._burst_window_start) > self.cfg.burst_coalesce_ms / 1000.0):
                        # 新窗口，清空候选池
                        self._burst_window_candidates = []
                        self._burst_window_start = ts
                    
                    # 添加到候选池
                    self._burst_window_candidates.append({
                        'signal': signal,
                        'score': abs(fusion_score),
                        'ts': ts
                    })
                return SignalType.NEUTRAL, denoising_reasons
        
        # 更新方向记录
        if signal != SignalType.NEUTRAL:
            self._last_signal_direction = current_direction
        
        # 2. 一致性加权迟滞处理 - 增强版
        consistency_bonus = max(0.0, consistency - 0.3) * 0.5  # 一致性加分
        adjusted_hysteresis = self.cfg.hysteresis_exit + consistency_bonus
        
        if (self._last_signal == SignalType.STRONG_BUY and 
            signal == SignalType.BUY and 
            fusion_score > adjusted_hysteresis):
            # 从强买入降级到买入时，如果得分仍然很高，保持强买入
            denoising_reasons.append("hysteresis_hold")
            signal = SignalType.STRONG_BUY
        elif (self._last_signal == SignalType.STRONG_SELL and 
              signal == SignalType.SELL and 
              fusion_score < -adjusted_hysteresis):
            # 从强卖出降级到卖出时，如果得分仍然很低，保持强卖出
            denoising_reasons.append("hysteresis_hold")
            signal = SignalType.STRONG_SELL
        elif (self._last_signal in [SignalType.BUY, SignalType.STRONG_BUY] and 
              signal == SignalType.NEUTRAL and 
              fusion_score > adjusted_hysteresis):
            # 从买入信号变为中性时，如果得分仍然较高，保持买入
            denoising_reasons.append("hysteresis_hold")
            signal = self._last_signal
        elif (self._last_signal in [SignalType.SELL, SignalType.STRONG_SELL] and 
              signal == SignalType.NEUTRAL and 
              fusion_score < -adjusted_hysteresis):
            # 从卖出信号变为中性时，如果得分仍然较低，保持卖出
            denoising_reasons.append("hysteresis_hold")
            signal = self._last_signal
        
        # 3. 一致性驱动的信号强度调整
        if consistency > 0.7:  # 高一致性时放宽阈值
            # 允许将 NEUTRAL 升级为 BUY/SELL（接近阈值时）
            if signal == SignalType.NEUTRAL:
                if fusion_score > self.cfg.fuse_buy * 0.8:
                    signal = SignalType.BUY
                    denoising_reasons.append("consistency_boost")
                elif fusion_score < self.cfg.fuse_sell * 0.8:
                    signal = SignalType.SELL
                    denoising_reasons.append("consistency_boost")
        elif consistency < 0.3:  # 低一致性时严格节流
            if signal in [SignalType.BUY, SignalType.STRONG_BUY, SignalType.SELL, SignalType.STRONG_SELL]:
                signal = SignalType.NEUTRAL
                denoising_reasons.append("low_consistency_throttle")
        
        # 4. 最小持续检查：一致性提升（consistency_boost）允许放行首帧
        bypass = ("consistency_boost" in denoising_reasons)
        if signal != SignalType.NEUTRAL and self._streak < self.cfg.min_consecutive and not bypass:
            signal = SignalType.NEUTRAL
            denoising_reasons.append("min_duration")
            self._stats['min_duration_blocks'] += 1
        
        return signal, denoising_reasons
    
    def update(self, z_ofi: float, z_cvd: float, ts: float,
               price: Optional[float] = None, lag_sec: float = 0.0, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        更新融合信号
        
        Args:
            z_ofi: OFI Z-score
            z_cvd: CVD Z-score
            ts: 事件时间戳
            price: 可选价格信息
            lag_sec: OFI/CVD时间差(秒)
            meta: 可选的元数据字典，包含上游 warmup 状态信息
            
        Returns:
            融合结果字典
        """
        # [LAG_GATING_FIX_V1] 在方法开始时记录接收到的 lag_sec 值
        ts_ms = int(ts * 1000) if ts else None
        symbol = meta.get('symbol') if meta and isinstance(meta, dict) else None
        lag_is_replay_like = meta.get('lag_is_replay_like', False) if meta and isinstance(meta, dict) else False
        lag_is_fallback = meta.get('lag_is_fallback', False) if meta and isinstance(meta, dict) else False
        
        # 如果 lag_sec 很大，或者有 replay_like 标记，记录详细日志
        # [DIAG] 增强诊断：记录 lag_is_replay_like 的提取过程
        if lag_sec > 3.0 or lag_is_replay_like or (abs(lag_sec - 0.0) < 0.001 and lag_is_replay_like):
            self._logger.warning(
                f"[FUSION_UPDATE_ENTRY] ts_ms={ts_ms} symbol={symbol} "
                f"lag_sec={lag_sec:.3f} lag_is_replay_like={lag_is_replay_like} "
                f"lag_is_fallback={lag_is_fallback} meta={meta}"
            )
        # [DIAG] 如果 lag_sec=0.0 且 lag_is_replay_like=True，额外记录诊断日志
        if abs(lag_sec - 0.0) < 0.001 and lag_is_replay_like:
            self._logger.info(
                f"[FUSION_UPDATE_ENTRY][DIAG] lag_sec=0.0 且 lag_is_replay_like=True: "
                f"ts_ms={ts_ms}, symbol={symbol}, lag_sec={lag_sec}, lag_is_replay_like={lag_is_replay_like}, "
                f"meta_lag_is_replay_like={meta.get('lag_is_replay_like', 'NOT_IN_META') if meta else 'NO_META'}"
            )
        
        self._stats['total_updates'] += 1
        reason_codes = []
        
        # 1. 输入验证
        if any(x is None or math.isinf(x) or math.isnan(x) 
               for x in [z_ofi, z_cvd]):
            self._stats['invalid_inputs'] += 1
            return {
                "fusion_score": 0.0,
                "signal": SignalType.NEUTRAL.value,
                # P0-4: 添加新字段
                "consistency": 0.0,  # 兼容字段
                "consistency_value": 0.0,
                "consistency_mode": "invalid",
                "consistency_effective": 0.0,
                "fusion_version": "v2",  # 统一结构化输出
                "ofi_weight": self.w_ofi,  # 修复：使用实际权重而非0
                "cvd_weight": self.w_cvd,  # 修复：使用实际权重而非0
                "reason_codes": ["invalid_input"],
                "components": {"ofi": 0.0, "cvd": 0.0},
                "warmup": self._is_warmup,  # 修复：使用真实暖启动状态
                "stats": self._stats.copy()  # 修复：添加stats字段
            }
        
        # 2. 暖启动检查（返回 consistency=0.0）
        # [FIX] 与 FeatureEngine / FeatureState 对齐：若上游已 ready，则跳过 Fusion 自身 warmup
        # 典型现象：FeatureState is_ready=True / warmup=False，但 Fusion 仍 mode=warmup,codes=warmup
        force_ready = False
        external_warmup_done = None
        external_samples = None
        external_min_samples = None
        if meta and isinstance(meta, dict):
            force_ready = bool(meta.get("force_ready", False))
            # warmup_done=True 表示上游认为已经出 warmup
            external_warmup_done = meta.get("warmup_done", None)
            external_samples = meta.get("warmup_samples", None)
            external_min_samples = meta.get("warmup_min_samples", None)
        
        # 用 external_samples 判定 warmup（如果提供了）
        if external_samples is not None:
            try:
                ms = int(external_min_samples) if external_min_samples is not None else int(getattr(self.cfg, "min_warmup_samples", 0) or 0)
                if ms > 0:
                    external_warmup_done = (int(external_samples) >= ms)
            except Exception:
                pass
        
        warmup_hit = (self._check_warmup() if not force_ready else False)
        # 若上游明确 warmup_done=True，则覆盖 warmup_hit
        if external_warmup_done is True:
            warmup_hit = False
        
        if warmup_hit:
            self._stats['warmup_returns'] += 1
            return {
                "fusion_score": 0.0,
                "signal": SignalType.NEUTRAL.value,
                # P0-4: 添加新字段
                "consistency": 0.0,  # 兼容字段
                "consistency_value": 0.0,
                "consistency_mode": "warmup",
                "consistency_effective": 0.0,
                "fusion_version": "v2",  # 统一结构化输出
                "ofi_weight": self.w_ofi,
                "cvd_weight": self.w_cvd,
                "reason_codes": ["warmup"],
                "components": {"ofi": 0.0, "cvd": 0.0},
                "warmup": True,
                "stats": self._stats.copy()  # 修复：添加stats字段
            }
        
        # 3. 数据裁剪
        z_ofi_clipped = self._clip_z_score(z_ofi)
        z_cvd_clipped = self._clip_z_score(z_cvd)
        
        # [P1 FIX] 从meta中获取symbol（用于日志降噪）
        symbol = None
        if meta and isinstance(meta, dict):
            symbol = meta.get('symbol')
        
        # 4. 时间对齐检查与单因子降级
        w_ofi, w_cvd = self.w_ofi, self.w_cvd
        degraded = False
        # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 检查之前是否处于降级模式
        # [FIX] 即使 symbol 为 None，也要检查全局降级状态
        state_key = symbol if symbol else "__GLOBAL__"
        was_degraded = self._last_degraded_state.get(state_key, False)
        
        # [LAG_GATING_FIX_V1] Patch C: 增加 lag gating 的决策日志
        # 注意：lag_is_fallback 和 lag_is_replay_like 已经在方法开始处（第 533-534 行）提取过了
        # 这里不需要重新提取，直接使用之前的值
        # lag_is_fallback 和 lag_is_replay_like 已经在第 533-534 行提取，这里不需要重复
        # ts_ms 也已经在第 531 行计算过了，这里不需要重复
        
        if lag_sec > self.cfg.max_lag:
            self._stats['lag_exceeded'] += 1
            self._stats['downgrades'] += 1  # 添加降级统计
            reason_codes.append("lag_exceeded")
            # 实现单因子降级：保留更"强"的一侧
            if abs(z_ofi_clipped) >= abs(z_cvd_clipped):
                w_ofi, w_cvd = 1.0, 0.0
                reason_codes.append("degraded_ofi_only")
                decision = "CAP"  # 降级但继续处理
            else:
                w_ofi, w_cvd = 0.0, 1.0
                reason_codes.append("degraded_cvd_only")
                decision = "CAP"  # 降级但继续处理
            degraded = True
            
            # [LAG_GATING_FIX_V1] 记录 lag gating 决策日志
            self._logger.warning(
                f"[FUSION_LAG][{decision}] sym={symbol} ts_ms={ts_ms} lag_sec={lag_sec:.3f} "
                f"max_lag={self.cfg.max_lag} lag_is_fallback={lag_is_fallback} "
                f"replay_like={lag_is_replay_like} degraded=True"
            )
        else:
            decision = "PASS"
            # [LAG_GATING_FIX_V1] 记录通过日志（仅在 lag_is_fallback 或 lag_is_replay_like 为 True 时记录，避免日志过多）
            if lag_is_fallback or lag_is_replay_like:
                self._logger.info(
                    f"[FUSION_LAG][{decision}] sym={symbol} ts_ms={ts_ms} lag_sec={lag_sec:.3f} "
                    f"max_lag={self.cfg.max_lag} lag_is_fallback={lag_is_fallback} "
                    f"replay_like={lag_is_replay_like}"
                )
        
        # 5. 融合计算
        raw_fusion = w_ofi * z_ofi_clipped + w_cvd * z_cvd_clipped
        
        # P0-4: 计算 consistency 的三个字段（value/mode/effective）
        consistency_value = self._consistency(z_ofi_clipped, z_cvd_clipped)  # 真实公式值
        
        # [OBS] 当 consistency_value=0.0 时补充原因码（之前只输出了数值，无法区分 warmup/缺失/方向冲突/过小）
        # 注意：warmup/invalid 已在前面 return；这里仅覆盖"正常路径但算出来为0"的两类主因
        if consistency_value == 0.0:
            rc = self._consistency_reason(z_ofi_clipped, z_cvd_clipped)
            if rc not in reason_codes:
                reason_codes.append(rc)
        
        # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 检查是否应该退出降级模式
        recovery_reason = None
        # [DIAG] 确保属性存在（防御性编程）
        if not hasattr(self, '_degraded_recovery_enabled'):
            self._degraded_recovery_enabled = True
            self._degraded_recovery_replay_like_exit = True
            self._degraded_recovery_normal_lag_threshold = 5
        
        # [FIX] 恢复逻辑：检查之前是否处于降级模式，如果满足恢复条件则退出
        # 注意：即使当前 degraded=False（因为 lag_sec <= max_lag），如果之前处于降级模式，也应该检查是否应该退出
        # [DIAG] 诊断日志：记录恢复条件检查（使用 INFO 级别以便查看）
        # 使用浮点数比较，避免精度问题
        if abs(lag_sec - 0.0) < 0.001 and lag_is_replay_like:
            self._logger.info(
                f"[FUSION][DEGRADED][RECOVERY][CHECK] 检查恢复条件: "
                f"symbol={symbol}, ts_ms={ts_ms}, lag_sec={lag_sec}, lag_is_replay_like={lag_is_replay_like}, "
                f"was_degraded={was_degraded}, degraded={degraded}, "
                f"recovery_enabled={self._degraded_recovery_enabled}, "
                f"replay_like_exit={self._degraded_recovery_replay_like_exit}"
            )
        
        if (degraded or was_degraded) and self._degraded_recovery_enabled:
            # 如果 lag_sec=0.0 且 replay_like=True，退出降级模式
            # 使用浮点数比较，避免精度问题
            if abs(lag_sec - 0.0) < 0.001 and lag_is_replay_like and self._degraded_recovery_replay_like_exit:
                degraded = False
                recovery_reason = "replay_like_exit"
                # [DIAG] 诊断日志：记录恢复触发
                self._logger.info(
                    f"[FUSION][DEGRADED][RECOVERY] 触发恢复: reason={recovery_reason}, "
                    f"symbol={symbol}, ts_ms={ts_ms}, lag_sec={lag_sec}, lag_is_replay_like={lag_is_replay_like}, was_degraded={was_degraded}"
                )
                # 恢复正常权重
                w_ofi, w_cvd = self.w_ofi, self.w_cvd
                # 从 reason_codes 中移除降级相关的代码
                if "lag_exceeded" in reason_codes:
                    reason_codes.remove("lag_exceeded")
                if "degraded_ofi_only" in reason_codes:
                    reason_codes.remove("degraded_ofi_only")
                if "degraded_cvd_only" in reason_codes:
                    reason_codes.remove("degraded_cvd_only")
            # 或者，如果连续 N 次 lag_sec <= max_lag，退出降级模式
            elif lag_sec <= self.cfg.max_lag:
                # [FIX] 即使 symbol 为 None，也要跟踪正常延迟计数
                count_key = symbol if symbol else "__GLOBAL__"
                if count_key not in self._normal_lag_count:
                    self._normal_lag_count[count_key] = 0
                self._normal_lag_count[count_key] += 1
                
                threshold = self._degraded_recovery_normal_lag_threshold
                if self._normal_lag_count[count_key] >= threshold:
                    degraded = False
                    recovery_reason = f"normal_lag_continuous_{self._normal_lag_count[count_key]}_threshold_{threshold}"
                    # [DIAG] 诊断日志：记录恢复触发
                    self._logger.info(
                        f"[FUSION][DEGRADED][RECOVERY] 触发恢复: reason={recovery_reason}, "
                        f"symbol={symbol}, ts_ms={ts_ms}, lag_sec={lag_sec}, count={self._normal_lag_count[count_key]}, threshold={threshold}, was_degraded={was_degraded}"
                    )
                    # 恢复正常权重
                    w_ofi, w_cvd = self.w_ofi, self.w_cvd
                    # 从 reason_codes 中移除降级相关的代码
                    if "lag_exceeded" in reason_codes:
                        reason_codes.remove("lag_exceeded")
                    if "degraded_ofi_only" in reason_codes:
                        reason_codes.remove("degraded_ofi_only")
                    if "degraded_cvd_only" in reason_codes:
                        reason_codes.remove("degraded_cvd_only")
                    # 重置计数器
                    self._normal_lag_count[count_key] = 0
            else:
                # 重置计数器（lag_sec > max_lag）
                count_key = symbol if symbol else "__GLOBAL__"
                if count_key in self._normal_lag_count:
                    self._normal_lag_count[count_key] = 0
        
        if degraded:
            # [P1-FIX] 单因子降级：不绕过 consistency 检查，使用 max(consistency_floor, consistency_value)
            consistency_mode = 'degraded_ofi_only' if w_ofi == 1.0 else 'degraded_cvd_only'
            # 使用 consistency_floor 作为下限，但不完全绕过 consistency_value
            consistency_floor = getattr(self.cfg, 'consistency_floor', 0.0) if hasattr(self.cfg, 'consistency_floor') else 0.0
            consistency_effective = max(consistency_floor, consistency_value)
            
            # [P1 FIX] Consistency日志降噪：只在状态变化时记录一次
            # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 记录降级模式状态变化
            current_time = time.time()
            prev_state = self._lag_exceeded_state.get(symbol, False)
            # [FIX] 使用 state_key 获取之前是否处于降级模式
            state_key = symbol if symbol else "__GLOBAL__"
            prev_degraded = self._last_degraded_state.get(state_key, False)
            
            if not prev_state:
                # 状态变化：从正常变为lag_exceeded，记录一次
                self._lag_exceeded_state[symbol] = True
                self._lag_exceeded_last_log_ts[symbol] = current_time
                self._logger.warning(
                    f"[CONSISTENCY] [P1 FIX] lag_exceeded状态变化（进入降级模式）: "
                    f"symbol={symbol}, lag_sec={lag_sec:.3f} > max_lag={self.cfg.max_lag}, "
                    f"mode={consistency_mode}, consistency_value={consistency_value:.3f}, "
                    f"effective={consistency_effective:.3f}"
                )
            
            # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 记录进入降级模式
            if not prev_degraded:
                if symbol:
                    self._degraded_start_ts[symbol] = ts
                self._logger.warning(
                    f"[FUSION][DEGRADED] 进入降级模式: symbol={symbol}, ts_ms={ts_ms}, "
                    f"lag_sec={lag_sec:.3f}, max_lag={self.cfg.max_lag}"
                )
            else:
                # 已在lag_exceeded状态，不记录每tick日志
                pass
            
            # [FIX] 更新降级状态（即使 symbol 为 None 也要更新，使用特殊键）
            state_key = symbol if symbol else "__GLOBAL__"
            self._last_degraded_state[state_key] = True
            
            # 更新统计
            if symbol not in self._lag_exceeded_stats:
                self._lag_exceeded_stats[symbol] = {"count": 0, "last_ts": current_time}
            self._lag_exceeded_stats[symbol]["count"] += 1
            self._lag_exceeded_stats[symbol]["last_ts"] = current_time
            
            # [P1 FIX] 定期输出汇总统计（每60秒一次）
            if current_time - self._lag_exceeded_summary_last_ts >= self._lag_exceeded_summary_interval_sec:
                self._lag_exceeded_summary_last_ts = current_time
                # 汇总所有symbol的lag_exceeded统计
                summary_lines = []
                for sym, stats in self._lag_exceeded_stats.items():
                    if stats["count"] > 0:
                        summary_lines.append(
                            f"{sym}: count={stats['count']}, last_ts={stats['last_ts']:.1f}"
                        )
                if summary_lines:
                    self._logger.info(
                        f"[CONSISTENCY] [P1 FIX] lag_exceeded汇总统计（过去60秒）: "
                        f"{', '.join(summary_lines)}"
                    )
                    # 重置统计（可选：保留历史或重置）
                    # self._lag_exceeded_stats = {}
            
            # [诊断日志] 记录单因子降级（包含 effective 计算说明）- 仅在debug级别
            self._logger.debug(
                f"[CONSISTENCY_DIAG] consistency={consistency_effective:.3f} (value={consistency_value:.3f}, mode={consistency_mode}, effective={consistency_effective:.3f}, floor={consistency_floor:.3f}, reason=degraded_single_factor): "
                f"lag_sec={lag_sec:.3f} > max_lag={self.cfg.max_lag}, "
                f"z_ofi={z_ofi_clipped:.3f}, z_cvd={z_cvd_clipped:.3f}, "
                f"degraded_to={'OFI_only' if w_ofi == 1.0 else 'CVD_only'}"
            )
            
            # [P1-FIX] 如果 consistency_value < consistency_min，记录 warning（仅在状态变化时）
            consistency_min = getattr(self.cfg, 'consistency_min', 0.2) if hasattr(self.cfg, 'consistency_min') else 0.2
            if consistency_value < consistency_min:
                reason_codes.append(f"consistency_value_low({consistency_value:.3f}<{consistency_min:.3f})")
                # [P1 FIX] 只在状态变化时记录warning，不再每tick输出
                if not prev_state:
                    self._logger.warning(
                    f"[CONSISTENCY_DIAG] 降级模式下 consistency_value 过低: "
                    f"value={consistency_value:.3f} < min={consistency_min:.3f}, "
                    f"effective={consistency_effective:.3f}, mode={consistency_mode}"
                )
        else:
            # 正常情况
            consistency_mode = 'normal'
            consistency_effective = consistency_value
            
            # [P1 FIX] 如果之前处于lag_exceeded状态，现在恢复正常，记录状态变化
            # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 记录退出降级模式
            if symbol:
                prev_state = self._lag_exceeded_state.get(symbol, False)
                # [FIX] 使用 state_key 获取之前是否处于降级模式
                state_key = symbol if symbol else "__GLOBAL__"
                prev_degraded = self._last_degraded_state.get(state_key, False)
                
                if prev_state:
                    # 状态变化：从lag_exceeded恢复为正常，记录一次
                    self._lag_exceeded_state[symbol] = False
                    current_time = time.time()
                    self._lag_exceeded_last_log_ts[symbol] = current_time
                    self._logger.info(
                        f"[CONSISTENCY] [P1 FIX] lag_exceeded状态恢复（退出降级模式）: "
                        f"symbol={symbol}, lag_sec={lag_sec:.3f} <= max_lag={self.cfg.max_lag}"
                    )
                
                # [TASK-FUSION-DEGRADED-RECOVERY-V1.0] 记录退出降级模式
                if prev_degraded:
                    duration = ts - self._degraded_start_ts.get(symbol, ts)
                    self._logger.info(
                        f"[FUSION][DEGRADED] 退出降级模式: symbol={symbol}, ts_ms={ts_ms}, "
                        f"duration={duration:.1f}s, reason={recovery_reason or 'normal_lag'}"
                    )
                    # 清理状态
                    if symbol in self._degraded_start_ts:
                        del self._degraded_start_ts[symbol]
                
                # 更新降级状态（即使 symbol 为 None 也要更新）
                state_key = symbol if symbol else "__GLOBAL__"
                self._last_degraded_state[state_key] = False
        
        # 兼容字段：保持现有行为不变
        consistency = consistency_effective
        
        # 融合分数诊断日志（每10秒汇总一次）
        current_time = time.time()
        if not hasattr(self, '_last_fusion_log'):
            self._last_fusion_log = current_time
            self._fusion_samples = []
        
        # 收集样本用于统计
        self._fusion_samples.append({
            'z_ofi': z_ofi_clipped,
            'z_cvd': z_cvd_clipped,
            'w_ofi': w_ofi,
            'w_cvd': w_cvd,
            'raw_fusion': raw_fusion,
            'consistency': consistency,
            'ts': current_time
        })
        
        # 每10秒打印一次融合分数统计（带numpy try/except保护）
        if current_time - self._last_fusion_log >= 10:
            if self._fusion_samples:
                try:
                    import numpy as np
                    raw_fusions = [s['raw_fusion'] for s in self._fusion_samples]
                    consistencies = [s['consistency'] for s in self._fusion_samples]
                    
                    self._logger.info(f"[FUSION_DIAG] n={len(self._fusion_samples)} samples")
                    self._logger.info(f"[FUSION_DIAG] Raw fusion stats: p50={np.percentile(raw_fusions, 50):.3f}, "
                          f"p95={np.percentile(raw_fusions, 95):.3f}, p99={np.percentile(raw_fusions, 99):.3f}, max={np.max(raw_fusions):.3f}")
                    self._logger.info(f"[FUSION_DIAG] Consistency stats: p50={np.percentile(consistencies, 50):.3f}, "
                          f"p95={np.percentile(consistencies, 95):.3f}")
                    self._logger.info(f"[FUSION_DIAG] Sample: z_ofi={z_ofi_clipped:.3f}, z_cvd={z_cvd_clipped:.3f}, "
                          f"w_ofi={w_ofi:.2f}, w_cvd={w_cvd:.2f}, raw_fusion={raw_fusion:.3f}, consistency={consistency:.3f}")
                    
                    # 检查是否需要校准策略A
                    if np.percentile(raw_fusions, 95) < 0.3:
                        self._logger.warning("[FUSION_DIAG] Raw fusion p95 < 0.3, may need calibration strategy A")
                except ImportError:
                    self._logger.warning("[FUSION_DIAG] numpy not available, skipping statistics")
            else:
                self._logger.debug("[FUSION_DIAG] No samples collected in 10s window")
            
            self._last_fusion_log = current_time
            self._fusion_samples = []
        
        fusion_score = raw_fusion
        
        # 6. 信号生成
        signal = SignalType.NEUTRAL
        if (fusion_score > self.cfg.fuse_strong_buy and 
            consistency > self.cfg.strong_min_consistency):
            signal = SignalType.STRONG_BUY
        elif (fusion_score > self.cfg.fuse_buy and 
              consistency > self.cfg.min_consistency):
            signal = SignalType.BUY
        elif (fusion_score < self.cfg.fuse_strong_sell and 
              consistency > self.cfg.strong_min_consistency):
            signal = SignalType.STRONG_SELL
        elif (fusion_score < self.cfg.fuse_sell and 
              consistency > self.cfg.min_consistency):
            signal = SignalType.SELL
        
        # 7. 去噪处理 - 传递一致性参数
        signal, denoising_reasons = self._apply_denoising(signal, fusion_score, ts, consistency)
        reason_codes.extend(denoising_reasons)
        
        # 8. 状态更新
        if signal != SignalType.NEUTRAL:
            self._last_emit_ts = ts
        self._last_signal = signal if signal != SignalType.NEUTRAL else self._last_signal
        
        # 添加融合器可观测性护栏（受verbose控制）
        if self._verbose:
            self._logger.debug(f"[FUSION_OBSERVABILITY] Input: z_ofi={z_ofi_clipped:.6f}, z_cvd={z_cvd_clipped:.6f}")
            self._logger.debug(f"[FUSION_OBSERVABILITY] Weights: w_ofi={w_ofi:.3f}, w_cvd={w_cvd:.3f}")
            self._logger.debug(f"[FUSION_OBSERVABILITY] Raw fusion: {raw_fusion:.6f}, consistency={consistency:.3f}")
            self._logger.debug(f"[FUSION_OBSERVABILITY] Regime: {self._current_regime}, thresholds: fuse_buy={self.cfg.fuse_buy}, fuse_sell={self.cfg.fuse_sell}")
            self._logger.debug(f"[FUSION_OBSERVABILITY] Consistency thresholds: min={self.cfg.min_consistency:.3f}, strong={self.cfg.strong_min_consistency:.3f}")
            self._logger.debug(f"[FUSION_OBSERVABILITY] Config source: unified_config={self._config_loader is not None}")
        
        # 当融合分数接近0时，记录详细原因
        if abs(raw_fusion) < 1e-6:
            reason_code = "due_to_warmup" if "warmup" in reason_codes else "zero_inputs" if (abs(z_ofi_clipped) < 1e-6 and abs(z_cvd_clipped) < 1e-6) else "consistency_fail"
            self._logger.debug(f"[FUSION_ZERO] Raw fusion={raw_fusion:.6f}, reason_code={reason_code}, reason_codes={reason_codes}")
        
        self._logger.debug(f"[FUSION_INTERNAL] Signal: {signal.value}, reason_codes: {reason_codes}")
        
        return {
            "fusion_score": fusion_score,
            "signal": signal.value,
            # P0-4: 添加新字段（value/mode/effective）
            "consistency": consistency,  # 兼容字段 = consistency_effective
            "consistency_value": consistency_value,  # 真实公式值
            "consistency_mode": consistency_mode,  # 状态标识
            "consistency_effective": consistency_effective,  # 用于 gating 的值
            "fusion_version": "v2",  # 统一结构化输出
            "ofi_weight": w_ofi,
            "cvd_weight": w_cvd,
            "reason_codes": reason_codes,
            "components": {
                "ofi": w_ofi * z_ofi_clipped, 
                "cvd": w_cvd * z_cvd_clipped
            },
            "warmup": False,
            "stats": self._stats.copy(),
            "last_signal": self._last_signal.value,  # 上一次发射的非中性信号
            "streak": self._streak  # 当前连击计数
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self._stats.copy()
    
    def reset(self):
        """重置状态"""
        self._last_signal = SignalType.NEUTRAL
        self._last_emit_ts = None
        self._streak = 0
        self._prev_raw_signal = None  # 重置原始判定信号
        self._warmup_count = 0
        self._is_warmup = True
        # default regime for consistency thresholds
        self._current_regime = 'normal'
        
        # 高级机制状态重置
        self._last_signal_direction = 0
        self._burst_window_candidates = []
        self._burst_window_start = None
        
        self._stats = {
            'total_updates': 0,
            'downgrades': 0,
            'warmup_returns': 0,
            'invalid_inputs': 0,
            'lag_exceeded': 0,
            'cooldown_blocks': 0,
            'min_duration_blocks': 0,
            'flip_rearm': 0,
            'adaptive_cooldown_used': 0,
            'burst_coalesced': 0
        }
    
    # -------- 运行时更新（移入类内，修正缩进/作用域） --------
    def set_thresholds(self, **kwargs):
        """Update fusion thresholds and consistency safely at runtime.
        Accepts keys: fuse_buy, fuse_strong_buy, fuse_sell, fuse_strong_sell,
        min_consistency, strong_min_consistency, regime_consistency (dict),
        w_ofi, w_cvd.
        """
        updated = {}
        # update weights
        for k in ("w_ofi", "w_cvd"):
            if k in kwargs:
                try:
                    setattr(self.cfg, k, float(kwargs[k]))
                    updated[k] = float(kwargs[k])
                except Exception as e:
                    self._logger.warning(f"Failed to set {k}: {e}")
        # renormalize weights
        try:
            total = self.cfg.w_ofi + self.cfg.w_cvd
            if total > 0:
                self.w_ofi = self.cfg.w_ofi / total
                self.w_cvd = self.cfg.w_cvd / total
        except Exception as e:
            self._logger.warning(f"Weight renormalization failed: {e}")
        # thresholds
        for k in ("fuse_buy", "fuse_strong_buy", "fuse_sell", "fuse_strong_sell",
                  "min_consistency", "strong_min_consistency"):
            if k in kwargs:
                try:
                    setattr(self.cfg, k, float(kwargs[k]))
                    updated[k] = float(kwargs[k])
                except Exception as e:
                    self._logger.warning(f"Failed to set {k}: {e}")
        # regime consistency
        rc = kwargs.get("regime_consistency")
        if isinstance(rc, dict):
            try:
                self._regime_consistency = rc  # store for runtime switching
                updated["regime_consistency"] = True
            except Exception as e:
                self._logger.warning(f"Failed to set regime_consistency: {e}")
        if updated:
            self._logger.info(f"Fusion thresholds updated: {updated}")
        return updated

    def set_regime(self, regime: str):
        """外部可更新当前场景标签（供统一配置使用）"""
        old_regime = self._current_regime
        self._current_regime = regime
        
        # TASK-G3-REGIME-2x2-ACTIVATION: 优先级：场景级 > Regime 级 > Global
        # 只有在没有场景级配置时才应用 regime 级别配置
        scenario_min = getattr(self, '_scenario_min_consistency', None)
        scenario_strong = getattr(self, '_scenario_strong_min_consistency', None)
        
        # 如果配置了场景一致性阈值，应用当前regime的阈值（仅在场景级未设置时）
        if self._regime_consistency and isinstance(self._regime_consistency, dict):
            regime_config = self._regime_consistency.get(regime, {})
            if regime_config:
                try:
                    # 更新当前regime对应的阈值（仅在场景级未设置时）
                    if 'min_consistency' in regime_config and scenario_min is None:
                        self.cfg.min_consistency = float(regime_config['min_consistency'])
                        self._logger.debug(f"Updated min_consistency for regime {regime}: {self.cfg.min_consistency}")
                    elif scenario_min is not None:
                        self.cfg.min_consistency = float(scenario_min)
                        self._logger.debug(f"Using scenario min_consistency: {self.cfg.min_consistency}")
                    
                    if 'strong_min_consistency' in regime_config and scenario_strong is None:
                        self.cfg.strong_min_consistency = float(regime_config['strong_min_consistency'])
                        self._logger.debug(f"Updated strong_min_consistency for regime {regime}: {self.cfg.strong_min_consistency}")
                    elif scenario_strong is not None:
                        self.cfg.strong_min_consistency = float(scenario_strong)
                        self._logger.debug(f"Using scenario strong_min_consistency: {self.cfg.strong_min_consistency}")
                except Exception as e:
                    self._logger.warning(f"Failed to apply regime thresholds for {regime}: {e}")
        
        if old_regime != regime:
            self._logger.info(f"Regime switched from {old_regime} to {regime}")
        
        return self._current_regime
    
    def set_scenario_consistency(self, min_consistency: float, strong_min_consistency: Optional[float] = None):
        """
        TASK-G3-REGIME-2x2-ACTIVATION: 设置场景级 consistency_min（优先级最高）
        
        Args:
            min_consistency: 场景级 min_consistency
            strong_min_consistency: 场景级 strong_min_consistency（可选）
        """
        self._scenario_min_consistency = float(min_consistency)
        if strong_min_consistency is not None:
            self._scenario_strong_min_consistency = float(strong_min_consistency)
        
        # 立即应用到配置
        self.cfg.min_consistency = float(min_consistency)
        if strong_min_consistency is not None:
            self.cfg.strong_min_consistency = float(strong_min_consistency)
        
        self._logger.debug(
            f"[Fusion] Set scenario consistency: min={min_consistency}, "
            f"strong={strong_min_consistency if strong_min_consistency is not None else 'unchanged'}"
        )


def create_fusion_config(**kwargs) -> OFICVDFusionConfig:
    """
    创建融合配置的便捷函数
    
    Args:
        **kwargs: 配置参数
        
    Returns:
        配置对象
    """
    return OFICVDFusionConfig(**kwargs)


# 默认配置层级

DEFAULT_CONFIG = OFICVDFusionConfig()
