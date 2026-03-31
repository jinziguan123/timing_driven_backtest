"""
测试策略

所有的买入和卖出信号都为 True，用于测试回测框架。
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


class Test_Strategy(BaseStrategy):
    """
    测试策略
    
    所有时间点的买入和卖出信号都为 True，用于测试回测框架的正确性。
    """
    
    def __init__(self, **kwargs):
        super().__init__(name="Test_Strategy")
        self.params = kwargs
        
    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        index = data_dict['close'].index
        columns = data_dict['close'].columns
        
        entries = pd.DataFrame(
            np.full((len(index), len(columns)), True), 
            index=index, 
            columns=columns
        )
        exits = pd.DataFrame(
            np.full((len(index), len(columns)), True), 
            index=index, 
            columns=columns
        )
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entries, "exits": exits}

