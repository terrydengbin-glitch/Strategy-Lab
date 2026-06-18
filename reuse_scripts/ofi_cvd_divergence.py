"""
OFI-CVD 背离检测模块

实现面向反转与风险提示的背离检测：
- 支持价格 vs OFI、价格 vs CVD、价格 vs Fusion 三条通道
- 提供正向（看涨）/负向（看跌）与隐藏背离（趋势延续）
- 生成事件化输出（含强度评分、参与的枢轴/窗口、理由标签）
- 具备去噪与频控，并通过回测量化有效性

背离检测实现说明：
- 采用双枢轴结构：通过价格枢轴对（高点对高点、低点对低点）识别背离模式
- 枢轴定义：在窗口中心点，价格在左右各 swing_L 个样本内为最高/最低点时确认为枢轴
- 背离分类：常规背离（价格与指标反向）和隐藏背离（价格与指标同向但幅度不同）
- 当前实现使用确认型背离（confirmed mode），需要等待右侧 swing_L 个样本确认枢轴
- 未来支持快速响应模式（fast mode），可提前发出未确认背离预警

内存管理：
- PivotDetector 使用有界 deque(maxlen=max_pivots) 存储历史枢轴，防止内存泄漏
- 超过 max_pivots 时自动丢弃最旧的枢轴，保持内存占用稳定

Author: V13 OFI+CVD AI System
Created: 2025-01-20
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any, Deque, Literal
from enum import Enum
from collections import deque


class DivergenceType(Enum):
    """背离类型枚举"""
    BULL_REGULAR = "bull_div"        # 看涨常规背离
    BEAR_REGULAR = "bear_div"        # 看跌常规背离
    HIDDEN_BULL = "hidden_bull"      # 隐藏看涨背离
    HIDDEN_BEAR = "hidden_bear"      # 隐藏看跌背离
    OFI_CVD_CONFLICT = "ofi_cvd_conflict"  # OFI-CVD冲突


@dataclass
class DivergenceConfig:
    """背离检测配置"""
    # 枢轴检测参数
    swing_L: int = 12                # 枢轴检测窗口长度（向后兼容，默认值，实际使用由 mode 决定）
    ema_k: int = 5                   # EMA平滑参数
    max_pivots: int = 100            # 最大枢轴存储数量（防止内存泄漏）
    
    # 模式配置（P1：为未来 fast/confirmed 模式预留）
    mode: Literal["confirmed", "fast"] = "confirmed"  # 背离检测模式
    swing_L_confirmed: int = 12      # 确认型背离的窗口长度（保守，用于中低频策略）
    swing_L_fast: int = 5            # 快速响应模式的窗口长度（用于高频策略，未来实现）
    
    # 强度阈值
    z_hi: float = 1.5                # 高强度阈值
    z_mid: float = 0.7               # 中等强度阈值
    
    # 去噪参数
    min_separation: int = 6          # 最小枢轴间距
    cooldown_secs: float = 1.0       # 冷却时间
    warmup_min: int = 100            # 暖启动最小样本数
    max_lag: float = 0.300           # 最大滞后时间
    
    # 融合参数
    use_fusion: bool = True          # 是否使用融合指标
    cons_min: float = 0.3            # 最小一致性阈值
    
    # 评分权重
    z_mag_weight: float = 0.35       # Z值强度权重
    pivot_weight: float = 0.25       # 枢轴有效性权重
    agree_weight: float = 0.25       # 方向一致性权重
    consis_weight: float = 0.15      # 一致性权重
    
    # 强度分级阈值
    strong_threshold: float = 70.0   # 强背离阈值
    normal_threshold: float = 50.0   # 普通背离阈值
    weak_threshold: float = 35.0     # 弱背离阈值


class PivotDetector:
    """
    持久化枢轴的检测器（中心点成熟即确认；历史可配对）
    
    枢轴定义：
    - 在滑动窗口的中心点，如果价格在左右各 window_size 个样本内为最高/最低点，则确认为枢轴
    - 确认为枢轴需要等待右侧 window_size 个样本，确保中心点"成熟"
    - 每个枢轴记录：全局索引、时间戳、价格值、指标值、以及是否为价格/指标的高/低点
    
    max_pivots 行为：
    - 使用 collections.deque(maxlen=max_pivots) 存储历史枢轴
    - 当枢轴数量超过 max_pivots 时，自动丢弃最旧的枢轴（FIFO）
    - 这确保了长时间运行时内存占用稳定，防止 OOM 风险
    
    swing_L 语义：
    - window_size 参数对应配置中的 swing_L（枢轴检测窗口长度）
    - swing_L 越大，确认枢轴需要等待的样本越多，信号越保守但越可靠
    - 当前默认使用 confirmed 模式（swing_L=12），未来支持 fast 模式（swing_L=5）
    """
    
    def __init__(self, window_size: int, max_pivots: int = 100):
        """
        初始化枢轴检测器
        
        Args:
            window_size: 枢轴检测窗口长度（swing_L），确认为枢轴需要等待右侧 window_size 个样本
            max_pivots: 最大枢轴存储数量，超过此数量会自动丢弃最旧的枢轴（防止内存泄漏）
        """
        self.window_size = int(window_size)
        self.price_buffer = deque(maxlen=self.window_size * 2 + 1)
        self.indicator_buffer = deque(maxlen=self.window_size * 2 + 1)
        self.timestamp_buffer = deque(maxlen=self.window_size * 2 + 1)
        # 新增：全局样本序号与"已确认枢轴"存储
        self._n = 0                      # 已接收样本数（0-based -> 最后索引是 self._n-1）
        self.pivots: Deque[Dict[str, Any]] = deque(maxlen=max_pivots)  # 历史枢轴（有界存储，自动丢弃旧值）
        # 优化：使用有界 deque 存储最近的时间戳，防止 _seen_ts 无限增长
        # 保留 max_pivots * 2 个时间戳，确保覆盖所有可能的重复检查
        self._seen_ts: Deque[float] = deque(maxlen=max_pivots * 2)  # 去重，按 ts（有界）
    
    def add_point(self, ts: float, price: float, indicator: float) -> None:
        """添加数据点"""
        self._n += 1
        self.timestamp_buffer.append(ts)
        self.price_buffer.append(price)
        self.indicator_buffer.append(indicator)
    
    def add_point_and_detect(self, ts: float, price: float, indicator: float) -> int:
        """添加数据点并在'中心点成熟'时确认枢轴；返回本次新增枢轴个数"""
        self.add_point(ts, price, indicator)

        if len(self.price_buffer) < self.window_size * 2 + 1:
            return 0  # 窗口还没满，中心点尚未成熟

        i = self.window_size  # 窗口中心
        prices = list(self.price_buffer)
        indicators = list(self.indicator_buffer)
        timestamps = list(self.timestamp_buffer)

        ts_c = timestamps[i]
        # 去重：避免同一中心点多次确认（优化：使用有界 deque，O(n) 查找但内存可控）
        if ts_c in self._seen_ts:
            return 0

        left_p = prices[i - self.window_size:i]
        right_p = prices[i + 1:i + self.window_size + 1]
        is_price_high = (prices[i] >= max(left_p) and prices[i] >= max(right_p))
        is_price_low  = (prices[i] <= min(left_p) and prices[i] <= min(right_p))

        left_z = indicators[i - self.window_size:i]
        right_z = indicators[i + 1:i + self.window_size + 1]
        is_ind_high = (indicators[i] >= max(left_z) and indicators[i] >= max(right_z))
        is_ind_low  = (indicators[i] <= min(left_z) and indicators[i] <= min(right_z))

        new_cnt = 0
        if is_price_high or is_price_low:
            # 计算"全局索引"：当前总样本数 self._n，中心点对应全局索引 = (self._n - 1) - self.window_size
            gidx = (self._n - 1) - self.window_size
            self.pivots.append({
                'index': gidx,                # !!! 供 min_separation 与评分使用（全局）
                'ts': ts_c,
                'price': prices[i],
                'indicator': indicators[i],
                'is_price_high': is_price_high,
                'is_price_low':  is_price_low,
                'is_indicator_high': is_ind_high,
                'is_indicator_low':  is_ind_low
            })
            self._seen_ts.append(ts_c)  # 优化：使用 append 而不是 add，自动丢弃旧值
            new_cnt = 1

        return new_cnt

    def get_all_pivots(self) -> List[Dict[str, Any]]:
        """获取全部历史枢轴（返回列表副本）"""
        return list(self.pivots)
    
    def get_pivots_iter(self):
        """
        获取枢轴迭代器（性能优化：避免复制整个 deque）
        
        用于只需要遍历枢轴的场景，避免 list(self.pivots) 的内存复制开销
        
        Returns:
            枢轴的迭代器
        """
        return iter(self.pivots)
    
    def find_pivots(self) -> List[Dict[str, Any]]:
        """查找枢轴点（保持向后兼容）"""
        return self.get_all_pivots()


class DivergenceDetector:
    """
    背离检测器
    
    功能说明：
    - 检测价格与指标（OFI、CVD、Fusion）之间的背离模式
    - 支持常规背离（看涨/看跌）和隐藏背离（趋势延续信号）
    - 使用双枢轴结构：通过价格枢轴对识别背离，在相同时间点读取指标值进行比较
    
    配置参数说明：
    - swing_L: 枢轴检测窗口长度（向后兼容参数，实际使用由 mode 决定）
    - mode: 背离检测模式，"confirmed"（确认型，默认）或 "fast"（快速响应，未来实现）
    - swing_L_confirmed: 确认型模式的窗口长度（默认12，保守，用于中低频策略）
    - swing_L_fast: 快速响应模式的窗口长度（默认5，用于高频策略，未来实现）
    - max_pivots: 最大枢轴存储数量（默认100），超过此数量自动丢弃最旧枢轴，防止内存泄漏
    
    当前实现：
    - 使用 confirmed 模式，需要等待右侧 swing_L_confirmed 个样本确认枢轴
    - 未来 fast 模式将支持未确认背离的提前预警
    
    内存管理：
    - 所有 PivotDetector 实例使用有界 deque(maxlen=max_pivots) 存储枢轴
    - 确保长时间运行时内存占用稳定，避免 OOM 风险
    """
    
    def __init__(self, config: DivergenceConfig = None, config_loader=None,
                 runtime_cfg: Optional[Dict[str, Any]] = None):
        """
        初始化背离检测器
        
        Args:
            config: 背离检测配置对象，默认None使用默认配置
            config_loader: 配置加载器实例（兼容旧接口，库式调用时不应使用）
            runtime_cfg: 运行时配置字典，库式调用时使用（优先于config_loader）
        """
        # 优先使用运行时配置字典（库式调用）
        if runtime_cfg is not None:
            divergence_cfg = runtime_cfg.get('divergence', {}) if isinstance(runtime_cfg, dict) else {}
            # 从运行时配置构建DivergenceConfig对象
            default = DivergenceConfig()
            self.cfg = DivergenceConfig(
                swing_L=divergence_cfg.get('swing_L', default.swing_L),
                ema_k=divergence_cfg.get('ema_k', default.ema_k),
                z_hi=divergence_cfg.get('z_hi', default.z_hi),
                z_mid=divergence_cfg.get('z_mid', default.z_mid),
                min_separation=divergence_cfg.get('min_separation', default.min_separation),
                cooldown_secs=divergence_cfg.get('cooldown_secs', default.cooldown_secs),
                warmup_min=divergence_cfg.get('warmup_min', default.warmup_min),
                max_lag=divergence_cfg.get('max_lag', default.max_lag),
                use_fusion=divergence_cfg.get('use_fusion', default.use_fusion),
                max_pivots=divergence_cfg.get('max_pivots', default.max_pivots),  # 新增：最大枢轴数量
                # 模式配置（P1：为 fast/confirmed 模式预留）
                mode=divergence_cfg.get('mode', default.mode),
                swing_L_confirmed=divergence_cfg.get('swing_L_confirmed', default.swing_L_confirmed),
                swing_L_fast=divergence_cfg.get('swing_L_fast', default.swing_L_fast),
                cons_min=divergence_cfg.get('cons_min', default.cons_min)
            )
        elif config_loader:
            # 从统一配置系统加载参数（兼容旧接口）
            self.cfg = self._load_from_config_loader(config_loader)
        else:
            self.cfg = config or DivergenceConfig()
        
        self._reset_state()
        
        # 根据模式选择实际的 swing_L（P1：为 fast/confirmed 模式预留）
        self._actual_swing_L = self._get_swing_L_by_mode()
        
        # 枢轴检测器（传入 max_pivots 防止内存泄漏）
        self.price_ofi_detector = PivotDetector(self._actual_swing_L, max_pivots=self.cfg.max_pivots)
        self.price_cvd_detector = PivotDetector(self._actual_swing_L, max_pivots=self.cfg.max_pivots)
        self.price_fusion_detector = PivotDetector(self._actual_swing_L, max_pivots=self.cfg.max_pivots) if self.cfg.use_fusion else None
        
        # 状态跟踪
        self._last_event_ts = 0.0
        self._last_event_type = None
        self._sample_count = 0
        
        # 细粒度冷却与去重
        self._last_emitted_pairs = {}  # key: f"{channel}:{kind}" -> (idx_a, idx_b)
        self._last_event_ts_by_key = {}  # 细粒度冷却: event_type+channel
    
    def _get_swing_L_by_mode(self) -> int:
        """
        根据模式选择实际的 swing_L 值（P1：为 fast/confirmed 模式预留）
        
        Returns:
            实际使用的 swing_L 值
        """
        if self.cfg.mode == "confirmed":
            return self.cfg.swing_L_confirmed
        elif self.cfg.mode == "fast":
            return self.cfg.swing_L_fast
        else:
            # 向后兼容：如果 mode 不是预期值，使用 swing_L_confirmed
            return self.cfg.swing_L_confirmed
    
    def _load_from_config_loader(self, config_loader) -> DivergenceConfig:
        """
        从统一配置系统加载背离检测参数
        
        Args:
            config_loader: 统一配置加载器实例
            
        Returns:
            DivergenceConfig: 背离检测配置对象
        """
        try:
            # 导入背离检测配置加载器
            from config.divergence_config_loader import DivergenceConfigLoader
            
            # 创建背离检测配置加载器
            divergence_config_loader = DivergenceConfigLoader(config_loader)
            config = divergence_config_loader.load_config()
            
            # 创建DivergenceConfig对象
            return DivergenceConfig(
                swing_L=config.swing_L,
                ema_k=config.ema_k,
                z_hi=config.z_hi,
                z_mid=config.z_mid,
                min_separation=config.min_separation,
                cooldown_secs=config.cooldown_secs,
                warmup_min=config.warmup_min,
                max_lag=config.max_lag,
                use_fusion=config.use_fusion,
                max_pivots=getattr(config, "max_pivots", 100),  # 新增：最大枢轴数量
                # 模式配置（P1：为 fast/confirmed 模式预留）
                mode=getattr(config, "mode", "confirmed"),
                swing_L_confirmed=getattr(config, "swing_L_confirmed", 12),
                swing_L_fast=getattr(config, "swing_L_fast", 5),
                # 新增映射（若配置存在，否则使用默认值）
                cons_min=getattr(config, "cons_min", 0.3),
                z_mag_weight=getattr(config, "z_mag_weight", 0.35),
                pivot_weight=getattr(config, "pivot_weight", 0.25),
                agree_weight=getattr(config, "agree_weight", 0.25),
                consis_weight=getattr(config, "consis_weight", 0.15),
                strong_threshold=getattr(config, "strong_threshold", 70.0),
                normal_threshold=getattr(config, "normal_threshold", 50.0),
                weak_threshold=getattr(config, "weak_threshold", 35.0)
            )
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to load divergence detection config from config_loader: {e}. Using default config.")
            return DivergenceConfig()

    def _reset_state(self):
        """重置状态"""
        self._last_event_ts = 0.0
        self._last_event_type = None
        self._sample_count = 0
        self._last_event_ts_by_type = {}
        self._last_emitted_pairs = {}
        self._last_event_ts_by_key = {}
        self._stats = {
            'events_total': 0,
            'events_by_type': {dt.value: 0 for dt in DivergenceType},
            'suppressed_total': 0,
            'suppressed_by_reason': {},
            'soft_suppressed_total': 0,
            'pivots_detected': 0,
            'pivots_by_channel': {'ofi': 0, 'cvd': 0, 'fusion': 0},
            'last_update_ts': 0.0
        }
    
    def update(self, ts: float, price: float, z_ofi: float, z_cvd: float, 
               fusion_score: Optional[float] = None, consistency: Optional[float] = None,
               warmup: bool = False, lag_sec: float = 0.0) -> Optional[Dict[str, Any]]:
        """
        更新背离检测器
        
        Args:
            ts: 时间戳（秒）
            price: 价格
            z_ofi: OFI Z-score（裁剪到[-5,5]）
            z_cvd: CVD Z-score（裁剪到[-5,5]）
            fusion_score: 融合分数（可选）
            consistency: 一致性分数（可选）
            warmup: 是否在暖启动阶段
            lag_sec: 滞后时间（秒）
        
        Returns:
            背离事件字典或None
        """
        # 更新统计
        self._sample_count += 1
        self._stats['last_update_ts'] = ts
        
        # 背离检测降权机制（B阶段优化）
        confidence_multiplier = 1.0  # 默认置信度
        
        if not self._validate_input(ts, price, z_ofi, z_cvd, lag_sec):
            self._stats['suppressed_total'] += 1
            self._stats['suppressed_by_reason']['invalid_input'] = \
                self._stats['suppressed_by_reason'].get('invalid_input', 0) + 1
            return None  # 无效输入直接跳过
        elif warmup or self._sample_count < self.cfg.warmup_min:
            # 暖启动期间降权而非跳过
            confidence_multiplier = 0.3  # 降权到30%
            self._stats['suppressed_by_reason']['warmup'] = \
                self._stats['suppressed_by_reason'].get('warmup', 0) + 1
            self._stats['soft_suppressed_total'] = \
                self._stats.get('soft_suppressed_total', 0) + 1
        elif lag_sec > self.cfg.max_lag:
            # 延迟超限降权而非跳过
            confidence_multiplier = 0.5  # 降权到50%
            self._stats['suppressed_by_reason']['lag_exceeded'] = \
                self._stats['suppressed_by_reason'].get('lag_exceeded', 0) + 1
            self._stats['soft_suppressed_total'] = \
                self._stats.get('soft_suppressed_total', 0) + 1
        
        # 输入验证（已在上面处理，使用降权机制）
        # 暖启动和延迟检查（已在上面处理，使用降权机制）
        
        # 裁剪Z值
        z_ofi_clipped = max(-5.0, min(5.0, z_ofi))
        z_cvd_clipped = max(-5.0, min(5.0, z_cvd))
        
        # Fusion 裁剪到 Z 量纲（带合法性检查）
        fusion_score_clipped = None
        if isinstance(fusion_score, (int, float)) and math.isfinite(fusion_score):
            fusion_score_clipped = max(-5.0, min(5.0, float(fusion_score)))
        
        # 添加数据点并检测枢轴
        new_ofi = self.price_ofi_detector.add_point_and_detect(ts, price, z_ofi_clipped)
        new_cvd = self.price_cvd_detector.add_point_and_detect(ts, price, z_cvd_clipped)
        new_fus = 0
        if self.price_fusion_detector and fusion_score_clipped is not None:
            new_fus = self.price_fusion_detector.add_point_and_detect(ts, price, fusion_score_clipped)

        # 统计只加"新增数量"
        self._stats['pivots_detected'] += (new_ofi + new_cvd + new_fus)
        # 分通道统计
        self._stats['pivots_by_channel']['ofi'] += new_ofi
        self._stats['pivots_by_channel']['cvd'] += new_cvd
        self._stats['pivots_by_channel']['fusion'] += new_fus

        # 检测背离
        # 优化：不再需要预先获取所有枢轴，_detect_divergence 内部直接使用迭代器访问
        divergence_event = self._detect_divergence(
            ts, price, z_ofi_clipped, z_cvd_clipped, fusion_score_clipped, consistency,
            None, None, None, confidence_multiplier  # 参数保留以保持接口兼容，但不再使用
        )
        
        if divergence_event:
            self._stats['events_total'] += 1
            self._stats['events_by_type'][divergence_event['type']] += 1
            self._last_event_ts = ts
            self._last_event_type = divergence_event['type']
            # 按类型记录冷却时间
            self._last_event_ts_by_type[divergence_event['type']] = ts
        
        return divergence_event
    
    def _validate_input(self, ts: float, price: float, z_ofi: float, z_cvd: float, lag_sec: float) -> bool:
        """验证输入参数"""
        if not all(isinstance(x, (int, float)) and not math.isnan(x) and not math.isinf(x) 
                  for x in [ts, price, z_ofi, z_cvd, lag_sec]):
            return False
        if ts <= 0 or price <= 0:
            return False
        return True
    
    def _create_invalid_result(self, reason: str, warmup: bool) -> Optional[Dict[str, Any]]:
        """创建无效结果"""
        return {
            "ts": 0.0,
            "type": None,
            "score": 0.0,
            "channels": [],
            "lookback": {"swing_L": self.cfg.swing_L, "ema": self.cfg.ema_k},
            "pivots": {"price": {}, "ofi": {}, "cvd": {}, "fusion": {}},
            "reason_codes": [reason],
            "debug": {"z_ofi": 0.0, "z_cvd": 0.0, "fusion": 0.0, "consistency": 0.0},
            "warmup": warmup,
            "stats": self._stats.copy()
        }
    
    def _detect_divergence(self, ts: float, price: float, z_ofi: float, z_cvd: float,
                          fusion_score: Optional[float], consistency: Optional[float],
                          ofi_pivots: Optional[List[Dict]], cvd_pivots: Optional[List[Dict]], 
                          fusion_pivots: Optional[List[Dict]], confidence_multiplier: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        检测背离
        
        注意：ofi_pivots, cvd_pivots, fusion_pivots 参数已不再使用（保留以保持接口兼容）
        实际访问通过 detector.get_pivots_iter() 直接获取迭代器，避免内存复制
        """
        
        # 并行检测：先检测方向性背离，再检查冲突
        divergence_events = []
        
        # Price-OFI背离 - 使用新方法检测L和H
        evt, reason = self._check_price_indicator_divergence_new("ofi", self.price_ofi_detector, 'L')
        self._debug_tally(reason)
        if evt:
            divergence_events.append(evt)
            
        evt, reason = self._check_price_indicator_divergence_new("ofi", self.price_ofi_detector, 'H')
        self._debug_tally(reason)
        if evt:
            divergence_events.append(evt)
        
        # Price-CVD背离 - 使用新方法检测L和H
        evt, reason = self._check_price_indicator_divergence_new("cvd", self.price_cvd_detector, 'L')
        self._debug_tally(reason)
        if evt:
            divergence_events.append(evt)
            
        evt, reason = self._check_price_indicator_divergence_new("cvd", self.price_cvd_detector, 'H')
        self._debug_tally(reason)
        if evt:
            divergence_events.append(evt)
        
        # Price-Fusion背离 - 使用新方法检测L和H，传入 consistency
        if self.cfg.use_fusion and fusion_score is not None:
            evt, reason = self._check_price_indicator_divergence_new("fusion", self.price_fusion_detector, 'L', consistency)
            self._debug_tally(reason)
            if evt:
                divergence_events.append(evt)
                
            evt, reason = self._check_price_indicator_divergence_new("fusion", self.price_fusion_detector, 'H', consistency)
            self._debug_tally(reason)
            if evt:
                divergence_events.append(evt)
        
        # 选择最佳方向性背离事件
        best_directional_event = None
        if divergence_events:
            best_directional_event = max(divergence_events, key=lambda x: x['score'])
            
            # 使用细粒度冷却检查：基于 event_type + channel
            et = best_directional_event["type"]
            ch = best_directional_event.get("channel", "")
            if self._is_in_cooldown_key(ts, et, ch):
                self._stats['suppressed_total'] += 1
                self._stats['suppressed_by_reason']['cooldown'] = \
                    self._stats['suppressed_by_reason'].get('cooldown', 0) + 1
                best_directional_event = None
            else:
                self._mark_cooldown_key(ts, et, ch)
        
        # 检查OFI-CVD冲突（作为附加信息）
        conflict_event = self._check_ofi_cvd_conflict(z_ofi, z_cvd, ts)
        if conflict_event and not self._is_in_cooldown(ts, conflict_event['type']):
            # 如果有方向性背离，将冲突信息添加到reason_codes中
            if best_directional_event:
                best_directional_event['reason_codes'].append('ofi_cvd_conflict')
                best_directional_event['channels'].extend(['price_ofi', 'price_cvd'])
                # 提升分数以反映冲突的存在
                best_directional_event['score'] = min(best_directional_event['score'] + 10, 100)
            else:
                # 如果没有方向性背离，返回冲突事件
                return conflict_event
        
        # 应用置信度乘数（B阶段优化）
        if best_directional_event and confidence_multiplier < 1.0:
            best_directional_event['score'] = best_directional_event['score'] * confidence_multiplier
            best_directional_event['confidence'] = confidence_multiplier
            best_directional_event['reason_codes'].append(f'confidence_{confidence_multiplier:.1f}')
        
        return best_directional_event
    
    def _is_same_type_pivots(self, pivot_a: Dict, pivot_b: Dict) -> bool:
        """检查两个枢轴是否为同类型（都是高点或都是低点）"""
        # 检查价格枢轴类型
        price_same_type = (
            (pivot_a.get('is_price_high', False) and pivot_b.get('is_price_high', False)) or
            (pivot_a.get('is_price_low', False) and pivot_b.get('is_price_low', False))
        )
        
        # 检查指标枢轴类型
        indicator_same_type = (
            (pivot_a.get('is_indicator_high', False) and pivot_b.get('is_indicator_high', False)) or
            (pivot_a.get('is_indicator_low', False) and pivot_b.get('is_indicator_low', False))
        )
        
        return price_same_type and indicator_same_type
    
    def _last_two_price_pivots(self, pivots, kind: str) -> Optional[List[Dict[str, Any]]]:
        """
        获取最近两个同型价格枢轴
        
        优化：支持传入迭代器或列表，避免不必要的列表复制
        
        Args:
            pivots: 枢轴列表或迭代器
            kind: 'L' 表示低点，'H' 表示高点
        
        Returns:
            最近两个同型价格枢轴的列表，如果不足两个则返回 None
        """
        # 优化：如果传入的是 deque 或迭代器，直接迭代而不复制
        if kind == 'L':
            xs = [p for p in pivots if p.get('is_price_low')]
        else:
            xs = [p for p in pivots if p.get('is_price_high')]
        return xs[-2:] if len(xs) >= 2 else None
    
    def _classify_by_values(self, kind: str, pa: float, pb: float, ia: float, ib: float) -> Optional[str]:
        """按教科书定义分类（用指标值，而不是指标枢轴）"""
        if kind == 'L':  # 低点对低点
            # Regular Bull: price LL, indicator HL
            if pb < pa and ib > ia:
                return "bull_regular"
            # Hidden Bull: price HL, indicator LL
            if pb > pa and ib < ia:
                return "bull_hidden"
        else:  # 'H' 高点对高点
            # Regular Bear: price HH, indicator LH
            if pb > pa and ib < ia:
                return "bear_regular"
            # Hidden Bear: price LH, indicator HH
            if pb < pa and ib > ia:
                return "bear_hidden"
        return None
    
    def _debug_tally(self, reason: Optional[str]) -> None:
        """调试统计：记录跳过原因"""
        if reason is None:
            return
        self._stats.setdefault("divergence_skip_reasons", {})
        self._stats["divergence_skip_reasons"][reason] = \
            self._stats["divergence_skip_reasons"].get(reason, 0) + 1
    
    def _is_in_cooldown(self, ts: float, event_type: str) -> bool:
        """检查是否在冷却期（按事件类型分别冷却）"""
        if event_type not in self._last_event_ts_by_type:
            return False
        
        last_ts = self._last_event_ts_by_type[event_type]
        return (ts - last_ts) < self.cfg.cooldown_secs
    
    def _is_in_cooldown_key(self, ts: float, event_type: str, channel: str) -> bool:
        """细粒度冷却检查：基于 event_type + channel"""
        key = f"{event_type}:{channel}"
        last = self._last_event_ts_by_key.get(key)
        return (last is not None) and ((ts - last) < self.cfg.cooldown_secs)
    
    def _mark_cooldown_key(self, ts: float, event_type: str, channel: str):
        """标记细粒度冷却"""
        key = f"{event_type}:{channel}"
        self._last_event_ts_by_key[key] = ts
    
    def _check_ofi_cvd_conflict(self, z_ofi: float, z_cvd: float, ts: float) -> Optional[Dict[str, Any]]:
        """检查OFI-CVD冲突"""
        if abs(z_ofi) >= self.cfg.z_hi and abs(z_cvd) >= self.cfg.z_mid:
            if (z_ofi > 0 and z_cvd < 0) or (z_ofi < 0 and z_cvd > 0):
                return {
                    "ts": ts,
                    "type": DivergenceType.OFI_CVD_CONFLICT.value,
                    "score": 60.0,  # 固定中等分数
                    "channels": ["price_ofi", "price_cvd"],
                    "lookback": {"swing_L": self._actual_swing_L, "ema": self.cfg.ema_k},
                    "pivots": {"price": {}, "ofi": {}, "cvd": {}, "fusion": {}},
                    "reason_codes": ["ofi_cvd_conflict"],
                    "debug": {"z_ofi": z_ofi, "z_cvd": z_cvd, "fusion": 0.0, "consistency": 0.0},
                    "warmup": False,
                    "stats": self._stats.copy()
                }
        return None
    
    def _check_price_indicator_divergence_new(self, channel_name: str, detector, kind: str, 
                                              consistency: Optional[float] = None) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """新的背离检测方法：只用价格枢轴对，在相同时间点读取指标值"""
        # 1) 用历史"价格枢轴"做同型配对
        # 优化：使用迭代器避免复制整个 deque（性能优化）
        pivs = detector.get_pivots_iter()
        pair = self._last_two_price_pivots(pivs, kind)
        if not pair:
            return None, "not_enough_price_pivots"

        a, b = pair  # a 更早, b 更新
        if (b["index"] - a["index"]) < self.cfg.min_separation:
            return None, "too_close"

        # 已发对儿去重: 同一 channel+kind 的 (a,b) 不重复发
        dedup_key = f"{channel_name}:{kind}"
        if self._last_emitted_pairs.get(dedup_key) == (a["index"], b["index"]):
            return None, "duplicate_pair"

        # 2) 直接取同一时间点的"指标数值"做比较（注意：这些值已在枢轴里缓存）
        pa, pb = a["price"], b["price"]
        ia, ib = a["indicator"], b["indicator"]

        # 3) 分类（不要求"指标也形成枢轴"）
        div_type = self._classify_by_values(kind, pa, pb, ia, ib)
        if not div_type:
            return None, "no_pattern"

        # 4) 使用统一的评分函数，传入 consistency
        score = self._calculate_divergence_score(
            pivot_a=a, 
            pivot_b=b, 
            current_value=ib,  # 使用b点的指标值
            indicator_name=channel_name,
            consistency=consistency  # 传入外部 consistency
        )

        if score < self.cfg.weak_threshold:
            return None, "score_below"

        # 映射到标准事件类型
        type_mapping = {
            "bull_regular": "bull_div",
            "bear_regular": "bear_div", 
            "bull_hidden": "hidden_bull",
            "bear_hidden": "hidden_bear"
        }
        evt_type = type_mapping.get(div_type, div_type)
        
        # 构造 debug 字段：填充对应通道的指标值
        dbg = {
            "z_ofi": 0.0, 
            "z_cvd": 0.0, 
            "fusion": 0.0,
            "consistency": consistency or 0.0
        }
        if channel_name == "ofi":
            dbg["z_ofi"] = ib
        elif channel_name == "cvd":
            dbg["z_cvd"] = ib
        elif channel_name == "fusion":
            dbg["fusion"] = ib
        
        # 统一事件结构
        evt = {
            "ts": b["ts"],
            "type": evt_type,
            "divergence_type": div_type,
            "score": score,
            "channel": channel_name,
            "channels": [f"price_{channel_name}"],
            "pivot_index": b["index"],
            "reason_codes": [],
            "lookback": {"swing_L": self.cfg.swing_L, "ema": self.cfg.ema_k},
            "pivots": {
                "price": {"A": pa, "B": pb},
                channel_name: {"A": ia, "B": ib}
            },
            "debug": dbg,
            "warmup": (self._sample_count < self.cfg.warmup_min),
            "stats": self._stats.copy()
        }
        # 标记去重
        self._last_emitted_pairs[dedup_key] = (a["index"], b["index"])
        return evt, None

    def _calculate_divergence_score(self, pivot_a: Dict, pivot_b: Dict, 
                                  current_value: float, indicator_name: str,
                                  consistency: Optional[float] = None) -> float:
        """计算背离评分"""
        # 原子证据分
        pivot_validity = self._calculate_pivot_validity(pivot_a, pivot_b)
        
        # 使用枢轴处的强度而不是当前值
        z_magnitude = self._calculate_pivot_z_magnitude(pivot_a, pivot_b, indicator_name)
        
        direction_agree = 1.0  # 单通道总是同意，多通道时会在调用处计算
        consistency_bonus = self._calculate_consistency_bonus(consistency)
        vol_ok = 1.0  # 简化：暂不考虑成交量过滤
        
        # 事件分
        score = 100 * (
            self.cfg.z_mag_weight * z_magnitude +
            self.cfg.pivot_weight * pivot_validity +
            self.cfg.agree_weight * direction_agree +
            self.cfg.consis_weight * consistency_bonus
        ) * vol_ok
        
        return min(100.0, max(0.0, score))
    
    def _calculate_pivot_validity(self, pivot_a: Dict, pivot_b: Dict) -> float:
        """计算枢轴有效性"""
        # 基于枢轴间距和形态清晰度
        separation = abs(pivot_b['index'] - pivot_a['index'])
        separation_score = min(1.0, separation / (self.cfg.swing_L * 2))
        
        # 基于价格和指标的变化幅度
        price_change = abs(pivot_b['price'] - pivot_a['price']) / pivot_a['price']
        indicator_change = abs(pivot_b['indicator'] - pivot_a['indicator'])
        
        magnitude_score = min(1.0, (price_change + indicator_change) / 2)
        
        return (separation_score + magnitude_score) / 2
    
    def _calculate_z_magnitude(self, z_value: float) -> float:
        """计算Z值强度"""
        abs_z = abs(z_value)
        if abs_z >= self.cfg.z_hi:
            return 1.0
        elif abs_z >= self.cfg.z_mid:
            return 0.7
        else:
            return 0.3
    
    def _calculate_pivot_z_magnitude(self, pivot_a: Dict, pivot_b: Dict, indicator_name: str) -> float:
        """计算枢轴处的Z值强度"""
        # 使用两个枢轴中较大的Z值强度
        z_a = abs(pivot_a.get('indicator', 0))
        z_b = abs(pivot_b.get('indicator', 0))
        max_z = max(z_a, z_b)
        
        if max_z >= self.cfg.z_hi:
            return 1.0
        elif max_z >= self.cfg.z_mid:
            return 0.7
        else:
            return 0.3
    
    def _calculate_consistency_bonus(self, consistency: Optional[float]) -> float:
        """计算一致性加分"""
        if consistency is None or consistency < self.cfg.cons_min:
            return 0.0
        
        # 线性加分：consistency从cons_min到1.0，加分从0.05到0.15
        normalized_consistency = (consistency - self.cfg.cons_min) / (1.0 - self.cfg.cons_min)
        return 0.05 + normalized_consistency * 0.10
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return self._stats.copy()
    
    def reset(self):
        """重置检测器状态"""
        self._reset_state()
        # 注意：_reset_state() 已经初始化了 self._stats，无需重复设置
