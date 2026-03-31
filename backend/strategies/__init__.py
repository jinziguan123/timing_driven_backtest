"""
策略模块 - 存放各类交易策略

目录结构：
- trend/: 趋势跟踪类策略
- mean_reversion/: 均值回归类策略
- grid/: 网格交易类策略
- custom/: 其他自定义策略
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

# 从各子模块导入策略类
from strategies.trend import (
    SMA_Cross_Strategy,
    EXPMA_Signal1_Strategy,
    BIAS_MACD_JINZUAN,
    BIAS_EXPMA_Strategy,
)
from strategies.mean_reversion import RSI_Strategy
from strategies.grid import Grid_Strategy
from strategies.custom import Test_Strategy
from strategies.trend.week_ema import Week_EMA_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_TOP_ENTRANCE_Strategy
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13
from strategies.trend.fibonacci_ema import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14
from strategies.trend.fibonacci_ema import Fibonacci_EMA_CROSS_Strategy
# 策略注册表 - 便于动态查找
STRATEGY_REGISTRY = {
    'SMA_Cross': SMA_Cross_Strategy,
    'EXPMA_Signal1': EXPMA_Signal1_Strategy,
    'BIAS_MACD_JINZUAN': BIAS_MACD_JINZUAN,
    'BIAS_EXPMA': BIAS_EXPMA_Strategy,
    'RSI': RSI_Strategy,
    'Grid': Grid_Strategy,
    'Test': Test_Strategy,
    'Week_EMA': Week_EMA_Strategy,
    'Fibonacci_EMA': Fibonacci_EMA_Strategy,
    'Fibonacci_EMA_BIAS': Fibonacci_EMA_BIAS_Strategy,
    'Fibonacci_EMA_BIAS_Lock_Main_Up_Wave': Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy,
    'Fibonacci_EMA_TOP_ENTRANCE': Fibonacci_EMA_TOP_ENTRANCE_Strategy,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V2': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V3': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V4': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V5': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V6': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V7': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V8': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V9': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V10': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V11': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V12': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V13': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13,
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_V14': Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14,
    'Fibonacci_EMA_CROSS': Fibonacci_EMA_CROSS_Strategy,
}

__all__ = [
    # 趋势策略
    'SMA_Cross_Strategy',
    'EXPMA_Signal1_Strategy',
    'BIAS_MACD_JINZUAN',
    'BIAS_EXPMA_Strategy',
    # 均值回归策略
    'RSI_Strategy',
    # 网格策略
    'Grid_Strategy',
    # 自定义策略
    'Test_Strategy',
    'Week_EMA_Strategy',
    'Fibonacci_EMA_Strategy',
    'Fibonacci_EMA_BIAS_Strategy',
    'Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13',
    'Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14',
    'Fibonacci_EMA_CROSS_Strategy',
    # 注册表
    'STRATEGY_REGISTRY',
]


def get_strategy(name: str):
    """
    根据名称获取策略类
    
    Args:
        name: 策略名称
        
    Returns:
        策略类
        
    Raises:
        KeyError: 如果策略不存在
    """
    if name not in STRATEGY_REGISTRY:
        raise KeyError(f"策略 '{name}' 不存在。可用策略: {list(STRATEGY_REGISTRY.keys())}")
    return STRATEGY_REGISTRY[name]
