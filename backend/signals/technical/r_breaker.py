"""
R-Breaker策略

经典的日内交易策略，基于前一日的 OHLC 价格计算枢轴点和多个价格关卡。

设置中心枢轴点(Pivot Point): P = (REF(H,1) + REF(L,1) + REF(C,1)) / 3

阻力线（从高到低）：
- 突破买入价 (Break Buy):      B_Buy  = REF(H,1) + 2 * (P - REF(L,1))
- 观察卖出价 (Observe Sell):   O_Sell = P + (REF(H,1) - REF(L,1))
- 反转卖出价 (Reversal Sell):  R_Sell = 2 * P - REF(L,1)

支撑线（从高到低）：
- 反转买入价 (Reversal Buy):   R_Buy  = 2 * P - REF(H,1)
- 观察买入价 (Observe Buy):    O_Buy  = P - (REF(H,1) - REF(L,1))
- 突破卖出价 (Break Sell):     B_Sell = REF(L,1) - 2 * (REF(H,1) - P)

买入逻辑：
- 突破买入：股价上涨突破"突破买入价"，买入
- 反转买入：当日最低价曾低于"观察买入价"，且当前价格向上突破"反转买入价"，买入

卖出逻辑：
- 突破卖出：股价下跌跌破"突破卖出价"，卖出
- 反转卖出：当日最高价曾超过"观察卖出价"，且当前价格回落跌破"反转卖出价"，卖出
- 止盈止损：+20% / -5%
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
from signals._numba import numba_r_breaker_signals


class R_Breaker_SignalGenerator(BaseSignalGenerator):
    """
    R-Breaker 信号生成器
    
    一个经典的日内交易策略，结合趋势突破和反转逻辑。
    使用前一日的 OHLC 价格计算枢轴点和六个关键价格关卡。
    """
    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        """
        运行 R-Breaker 策略模拟
        
        Args:
            data_dict: OHLCV 数据字典，分钟线数据
            time_index: 时间索引
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        # 聚合到日线级别
        day_data_dict = merge_data(data_dict, '1D')
        
        # ==================== 计算枢轴点和价格关卡（基于前一日数据） ====================
        prev_high = data_utils.REF(day_data_dict['high'], 1)
        prev_low = data_utils.REF(day_data_dict['low'], 1)
        prev_close = data_utils.REF(day_data_dict['close'], 1)
        
        # 中心枢轴点
        pivot_point = (prev_high + prev_low + prev_close) / 3
        
        # 计算前一日的振幅
        prev_range = prev_high - prev_low
        
        # 阻力线（从高到低）
        break_buy_price = prev_high + 2 * (pivot_point - prev_low)      # 突破买入价
        observe_sell_price = pivot_point + prev_range                    # 观察卖出价
        reversal_sell_price = 2 * pivot_point - prev_low                # 反转卖出价
        
        # 支撑线（从高到低）
        reversal_buy_price = 2 * pivot_point - prev_high                # 反转买入价
        observe_buy_price = pivot_point - prev_range                     # 观察买入价
        break_sell_price = prev_low - 2 * (prev_high - pivot_point)     # 突破卖出价
        
        # 清理临时变量
        del prev_high, prev_low, prev_close, pivot_point, prev_range
        
        # ==================== 下放价格关卡到分钟线级别 ====================
        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()
        
        def reindex_to_minute(day_df: pd.DataFrame) -> pd.DataFrame:
            """将日线数据下放到分钟线级别"""
            day_df_normalized = day_df.copy()
            day_df_normalized.index = pd.to_datetime(day_df_normalized.index).normalize()
            reindexed = day_df_normalized.reindex(minute_index_normalized, method='ffill')
            reindexed.index = minute_index
            return reindexed
        
        break_buy_price_min = reindex_to_minute(break_buy_price)
        observe_sell_price_min = reindex_to_minute(observe_sell_price)
        reversal_sell_price_min = reindex_to_minute(reversal_sell_price)
        reversal_buy_price_min = reindex_to_minute(reversal_buy_price)
        observe_buy_price_min = reindex_to_minute(observe_buy_price)
        break_sell_price_min = reindex_to_minute(break_sell_price)
        
        # 清理日线级别的临时变量
        del break_buy_price, observe_sell_price, reversal_sell_price
        del reversal_buy_price, observe_buy_price, break_sell_price
        
        # ==================== 创建日期索引数组（用于识别新的一天） ====================
        # 将日期转换为整数索引，用于 Numba 函数识别日期边界
        day_dates = minute_index.normalize()
        unique_dates = day_dates.unique()
        date_to_idx = {date: idx for idx, date in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[date] for date in day_dates], dtype=np.int64)
        
        # ==================== 调用 Numba 核心函数生成信号 ====================
        close_values = data_dict['close'].values.astype(np.float64)
        high_values = data_dict['high'].values.astype(np.float64)
        low_values = data_dict['low'].values.astype(np.float64)
        
        entry_signal_values, exit_signal_values = numba_r_breaker_signals(
            close_values,
            high_values,
            low_values,
            break_buy_price_min.values.astype(np.float64),
            observe_sell_price_min.values.astype(np.float64),
            reversal_sell_price_min.values.astype(np.float64),
            reversal_buy_price_min.values.astype(np.float64),
            observe_buy_price_min.values.astype(np.float64),
            break_sell_price_min.values.astype(np.float64),
            day_indices
        )
        
        # ==================== 构建输出信号 DataFrame ====================
        entry_signal = pd.DataFrame(
            entry_signal_values, 
            index=data_dict['close'].index, 
            columns=data_dict['close'].columns
        )
        exit_signal = pd.DataFrame(
            exit_signal_values, 
            index=data_dict['close'].index, 
            columns=data_dict['close'].columns
        )
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}
