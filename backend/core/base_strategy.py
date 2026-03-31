"""
策略基类定义

所有具体策略类都需要继承 BaseStrategy 并实现 generate_signals 方法。
"""

from abc import ABC, abstractmethod
from typing import Dict, Tuple, Any
import pandas as pd


class BaseStrategy(ABC):
    """
    策略基类（抽象基类）
    
    所有交易策略的基类，定义了策略的通用接口。
    子类必须实现 generate_signals 方法来生成交易信号。
    """
    
    def __init__(self, name: str = "BaseStrategy"):
        """
        初始化策略
        
        Args:
            name: 策略名称，用于标识和日志记录
        """
        self.name = name
        self.params: Dict[str, Any] = {}
    
    @abstractmethod
    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        核心逻辑函数 - 生成交易信号
        
        Args:
            data_dict: 包含 OHLCV 数据的字典
                - 'close': 收盘价 DataFrame (index=时间, columns=股票代码)
                - 'open': 开盘价 DataFrame
                - 'high': 最高价 DataFrame
                - 'low': 最低价 DataFrame
                - 'volume': 成交量 DataFrame
                - 'amount': 成交额 DataFrame
        
        Returns:
            Tuple[trade_method, signal_output]:
                - trade_method: TRADE_METHOD 枚举，指定信号类型
                - signal_output: 信号数据字典
                    - 对于 BUY_AND_SELL_SIGNALS: {'entries': DataFrame, 'exits': DataFrame}
                    - 对于 SIZE_AND_PRICE: {'size': DataFrame, 'price': DataFrame}
        """
        pass
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}')"

