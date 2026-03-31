"""
BIAS + MACD 金钻信号生成器

日线: (BIAS < -10) & (MACD开金叉)
周线: CLOSE跌破JINZUAN
"""

import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from core.base_signal import BaseSignalGenerator
from constants import TRADE_METHOD
import data_utils
from data_manager import merge_data
from signals._numba import numba_generate_bias_macd_jinzuan_sell_signal


class BIAS_MACD_JINZUAN_SignalGenerator(BaseSignalGenerator):
    """
    BIAS + MACD 金钻信号生成器
    
    策略逻辑：
    日线条件：
    - DIFF_BIAS > 0 (BIAS55 - BIAS20 > 0)
    - BIAS20 上穿 0 轴
    
    周线条件：
    - CCI > 100
    
    卖出条件：
    - 止盈 +30%
    - 止损 -10%
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(params)
                
    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        """
        运行信号模拟
        
        Args:
            data_dict: OHLCV 数据字典
            time_index: 时间索引
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')
        
        # ==================== 日线信号计算 ====================
        # 日均价
        average_price = data_utils.IF(
            day_data_dict['volume'] > 0, 
            day_data_dict['amount'] / (day_data_dict['volume'] * 100), 
            day_data_dict['close']
        )
        
        # 均线
        ma20 = data_utils.MA(average_price, 20)
        ma55 = data_utils.MA(average_price, 55)
        
        # BIAS 计算
        bias20 = (average_price - ma20) / ma20 * 100
        bias55 = (average_price - ma55) / ma55 * 100
        diff_bias = bias55 - bias20
        
        # 日线条件
        cond1 = diff_bias > 0
        cond2 = data_utils.CROSS(bias20, pd.DataFrame(0, index=bias20.index, columns=bias20.columns))
        day_condition_1 = cond1 & cond2
        
        # 内存清理
        del average_price, ma20, ma55, bias20, bias55, diff_bias, cond1, cond2
        
        # ==================== 周线信号计算 ====================
        week_condition_1 = data_utils.CCI(
            week_data_dict['close'], 
            week_data_dict['high'], 
            week_data_dict['low'], 
            14
        ) > 100
        
        # 将周线信号下放到日线级别
        week_condition_1 = week_condition_1.reindex(day_data_dict['close'].index, method='bfill')
        
        # ==================== 合成买入信号 ====================
        entry_signal = day_condition_1 & week_condition_1
        
        # 下放到分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index
        
        del day_condition_1, week_condition_1
        
        # ==================== 生成卖出信号 ====================
        entry_signal = entry_signal.fillna(False).infer_objects(copy=False)
        entry_signal = entry_signal.astype(bool)
        
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float64)
        
        exit_signal_values = numba_generate_bias_macd_jinzuan_sell_signal(entry_signal_values, close_values)
        exit_signal = pd.DataFrame(exit_signal_values, index=data_dict['close'].index, columns=data_dict['close'].columns)
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}

