"""
R-Breaker策略
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
from signals import R_Breaker_SignalGenerator

class R_Breaker_Strategy(BaseStrategy):
    """
    R_Breaker_Strategy
    """
    def __init__(self):
        super().__init__(name="R_Breaker_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        signal_generator = R_Breaker_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)