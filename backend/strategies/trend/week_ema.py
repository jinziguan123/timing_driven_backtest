"""
周线级别的EMA信号，采用斐波那契数列
周期从小到大多头排列，股价上穿短期均线EMA，形成买入信号
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

import vectorbt as vbt
from typing import Dict, Tuple, Any
import pandas as pd

from core.base_strategy import BaseStrategy
from constants import TRADE_METHOD
from signals import Week_EMA_SignalGenerator

class Week_EMA_Strategy(BaseStrategy):
    """
    Week_EMA_Strategy
    """
    def __init__(self):
        super().__init__(name="Week_EMA_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        signal_generator = Week_EMA_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)