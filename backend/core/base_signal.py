"""
信号生成器基类定义

所有具体信号生成器类都需要继承 BaseSignalGenerator 并实现 run_simulation 方法。
"""

from abc import ABC, abstractmethod
from typing import Dict, Tuple, Any
import pandas as pd


class BaseSignalGenerator(ABC):
    """
    信号生成器基类（抽象基类）
    
    所有信号生成器的基类，定义了信号生成的通用接口。
    信号生成器负责根据市场数据计算具体的交易信号。
    """
    
    def __init__(self, params: Dict[str, Any] = None):
        """
        初始化信号生成器
        
        Args:
            params: 信号生成器参数字典
        """
        self.params = params or {}
    
    @abstractmethod
    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        """
        运行信号模拟
        
        Args:
            data_dict: 包含 OHLCV 数据的字典
            time_index: 时间索引
        
        Returns:
            Tuple[trade_method, signal_output]:
                - trade_method: TRADE_METHOD 枚举，指定信号类型
                - signal_output: 信号数据字典
        """
        pass
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(params={self.params})"

