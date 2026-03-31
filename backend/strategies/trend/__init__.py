"""
趋势跟踪类策略

包含基于趋势判断的交易策略：
- 均线交叉策略
- EXPMA 策略
- BIAS + MACD 策略
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from strategies.trend.sma_cross import SMA_Cross_Strategy
from strategies.trend.expma import EXPMA_Signal1_Strategy
from strategies.trend.bias_macd import BIAS_MACD_JINZUAN
from strategies.trend.bias_expma import BIAS_EXPMA_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14
from strategies.trend.fibonacci_ema import Fibonacci_EMA_CROSS_Strategy
__all__ = [
    'SMA_Cross_Strategy',
    'EXPMA_Signal1_Strategy',
    'BIAS_MACD_JINZUAN',
    'BIAS_EXPMA_Strategy',
    'Fibonacci_EMA_Strategy',
    'Fibonacci_EMA_BIAS_Strategy',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14',
    'Fibonacci_EMA_CROSS_Strategy',
]
