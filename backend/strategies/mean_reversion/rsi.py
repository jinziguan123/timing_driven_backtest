"""
RSI 超买超卖策略

买入信号：RSI 下穿超卖线
卖出信号：RSI 上穿超买线
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


class RSI_Strategy(BaseStrategy):
    """
    RSI 超买超卖策略
    
    使用相对强弱指标（RSI）生成交易信号。
    当 RSI 下穿超卖线时买入，当 RSI 上穿超买线时卖出。
    """
    
    def __init__(self, window: int = 14, lower: int = 30, upper: int = 70):
        """
        初始化策略
        
        Args:
            window: RSI 计算周期
            lower: 超卖线（买入阈值）
            upper: 超买线（卖出阈值）
        """
        super().__init__(name=f"RSI_{window}")
        self.window = window
        self.lower = lower
        self.upper = upper

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        close = data_dict['close']
        
        rsi = vbt.RSI.run(close, window=self.window)
        
        entries = rsi.rsi_crossed_below(self.lower)
        exits = rsi.rsi_crossed_above(self.upper)
        
        # 强制将列名还原为原始的股票代码
        entries.columns = close.columns
        exits.columns = close.columns
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entries, "exits": exits}

