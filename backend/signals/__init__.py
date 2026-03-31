"""
信号模块 - 存放各类信号生成器

目录结构：
- technical/: 技术指标信号
- _numba/: Numba 加速的底层函数
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from signals.technical import (
    GridPositionManager,
    BIAS_MACD_JINZUAN_SignalGenerator,
    BIAS_EXPMA_SignalGenerator,
    Week_EMA_SignalGenerator,
    Fibonacci_EMA_SignalGenerator,
    Fibonacci_EMA_BIAS_SignalGenerator,
    R_Breaker_SignalGenerator,
    Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_SignalGenerator,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator,
    Fibonacci_EMA_TOP_ENTRANCE_SignalGenerator,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V2,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V3,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V4,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V5,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V6,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V7,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V8,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V9,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V10,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V12,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V13,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V14,
    Fibonacci_EMA_CROSS_SignalGenerator,
)

__all__ = [
    'GridPositionManager',
    'BIAS_MACD_JINZUAN_SignalGenerator',
    'BIAS_EXPMA_SignalGenerator',
    'Week_EMA_SignalGenerator',
    'Fibonacci_EMA_SignalGenerator',
    'Fibonacci_EMA_BIAS_SignalGenerator',
    'R_Breaker_SignalGenerator',
    'Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_SignalGenerator',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator',
    'Fibonacci_EMA_TOP_ENTRANCE_SignalGenerator',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V2',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V3',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V4',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V5',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V6',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V7',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V8',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V9',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V10',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V12',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V13',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V14',
    'Fibonacci_EMA_CROSS_SignalGenerator',
]
