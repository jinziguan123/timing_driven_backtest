"""
周线级别的EMA信号，采用斐波那契数列
周期从小到大多头排列，股价上穿短期均线EMA，形成买入信号

EMA13 := EMA(CLOSE, 13);
EMA34 := EMA(CLOSE, 34);
EMA55 := EMA(CLOSE, 55);

{ 多头排列 }
COND1 := EMA13 > EMA34 AND EMA34 > EMA55;

{ 股价上穿短期均线 EMA13 }
COND2 := CROSS(CLOSE, EMA13);

{ 综合条件 }
COND1 AND COND2;
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
from signals._numba import numba_generate_week_ema_sell_signal


class Week_EMA_SignalGenerator(BaseSignalGenerator):
    """
    Week_EMA_SignalGenerator
    """
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        """
        run_simulation
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        ema55 = data_utils.EMA(week_data_dict['close'], 55)
        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        cond_ema = (ema13 > ema34) & (ema34 > ema55)
        
        # 股价上穿短期均线 EMA13
        cond_cross = data_utils.CROSS(week_data_dict['close'], ema13)
        del ema13, ema34, ema55
        
        # 特殊处理，将前55根k线设置为False
        cond_ema.iloc[:55] = False
        
        # 下放至日线级别
        cond_ema = cond_ema.reindex(day_data_dict['close'].index, method='bfill')
        
        # 合成买入信号
        entry_signal = cond_ema & cond_cross
        del cond_ema, cond_cross
        
        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index
        
        # 生成卖出信号
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float64)
        exit_signal_values = numba_generate_week_ema_sell_signal(entry_signal_values, close_values)
        exit_signal = pd.DataFrame(exit_signal_values, index=data_dict['close'].index, columns=data_dict['close'].columns)
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}