"""
双均线交叉策略 (SMA Cross)

买入信号：快线上穿慢线（黄金交叉）
卖出信号：快线下穿慢线（死亡交叉）
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


class SMA_Cross_Strategy(BaseStrategy):
    """
    双均线交叉策略
    
    使用两条不同周期的简单移动平均线（SMA）生成交易信号。
    当快线上穿慢线时买入，当快线下穿慢线时卖出。
    """
    
    def __init__(self, fast_window: int = 10, slow_window: int = 20):
        """
        初始化策略
        
        Args:
            fast_window: 快线周期
            slow_window: 慢线周期
        """
        super().__init__(name=f"SMA_Cross_{fast_window}_{slow_window}")
        self.fast_window = fast_window
        self.slow_window = slow_window

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        close = data_dict['close']
        
        # 使用 vectorbt 的快速指标计算
        fast_ma = vbt.MA.run(close, window=self.fast_window)
        slow_ma = vbt.MA.run(close, window=self.slow_window)
        
        # 生成信号
        # 黄金交叉买入
        entries = fast_ma.ma_crossed_above(slow_ma)
        # 死亡交叉卖出
        exits = fast_ma.ma_crossed_below(slow_ma)
        
        # 强制将列名还原为原始的股票代码
        # 去除 vbt 自动添加的 (fast_window, slow_window) 索引层
        entries.columns = close.columns
        exits.columns = close.columns
        
        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entries, "exits": exits}

