"""
EXPMA 信号1策略

日线级别计算 EXPMA 和 KDJ 信号，然后映射回分钟线。
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Any

from core.base_strategy import BaseStrategy
from constants import TRADE_METHOD
from data_utils import EMA, SMA, LLV, HHV, REF


class EXPMA_Signal1_Strategy(BaseStrategy):
    """
    EXPMA信号1策略实现 (分钟线版本)
    
    逻辑：
    1. 将分钟线合成为日线
    2. 在日线级别计算 EXPMA 和 KDJ 信号
    3. 将日线信号映射回分钟线 (注意避免未来函数)
    """
    
    def __init__(self, exp1: int = 60, exp2: int = 120, 
                 kdj_period: int = 9, kdj_smooth1: int = 3, kdj_smooth2: int = 3):
        """
        初始化策略
        
        Args:
            exp1: EXPMA 快线周期
            exp2: EXPMA 慢线周期
            kdj_period: KDJ 的 RSV 周期
            kdj_smooth1: K 值平滑周期
            kdj_smooth2: D 值平滑周期
        """
        super().__init__(name="EXPMA_Signal1")
        self.exp1 = exp1
        self.exp2 = exp2
        self.kdj_period = kdj_period
        self.kdj_smooth1 = kdj_smooth1
        self.kdj_smooth2 = kdj_smooth2

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        # 1. 获取分钟线原始索引 (用于最后还原)
        minute_index = data_dict['close'].index
        
        # 2. 【降维】将分钟线重采样为日线
        close_daily = data_dict['close'].resample('1D').last().dropna(how='all')
        high_daily = data_dict['high'].resample('1D').max().dropna(how='all')
        low_daily = data_dict['low'].resample('1D').min().dropna(how='all')
        
        # 内存优化
        close_daily = close_daily.astype(np.float32)
        high_daily = high_daily.astype(np.float32)
        low_daily = low_daily.astype(np.float32)
        
        # 3. 【计算】在日线级别计算指标
        # --- EXPMA 计算 ---
        ma1 = EMA(close_daily, self.exp1).round(2)
        ma2 = EMA(close_daily, self.exp2).round(2)
        
        cond1 = ma1 > ma2
        cond2 = ma2 > REF(ma2, 1)
        
        # --- KDJ 计算 ---
        llv_low = LLV(low_daily, self.kdj_period)
        hhv_high = HHV(high_daily, self.kdj_period)
        denominator = hhv_high - llv_low
        denominator = denominator.replace(0, np.nan)
        
        rsv = ((close_daily - llv_low) / denominator * 100).round(2)
        
        # K, D
        k = SMA(rsv, self.kdj_smooth1, 1).round(2)
        d = SMA(k, self.kdj_smooth2, 1).round(2)
        
        # Signal Line 1
        signal_line_1 = (3 * k - 2 * d).round(2)
        
        # KDJ 条件
        sig_prev = REF(signal_line_1, 1)
        sig_prev2 = REF(signal_line_1, 2)
        
        cond3 = (sig_prev < 3) & (sig_prev < sig_prev2) & (signal_line_1 > sig_prev)
        
        # 4. 【合成日线信号】
        entries_daily = cond1 & cond2 & cond3
        exits_daily = ma1 < ma2 
        
        # 5. 【防未来函数】
        entries_daily = entries_daily.shift(1).fillna(False).infer_objects(copy=False)
        exits_daily = exits_daily.shift(1).fillna(False).infer_objects(copy=False)
        
        # 6. 【升维】将日线信号广播回 1分钟线
        entries_1min = entries_daily.reindex(minute_index, method='ffill').fillna(False)
        exits_1min = exits_daily.reindex(minute_index, method='ffill').fillna(False)
        
        # 确保列名正确
        entries_1min.columns = data_dict['close'].columns
        exits_1min.columns = data_dict['close'].columns
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entries_1min, "exits": exits_1min}

