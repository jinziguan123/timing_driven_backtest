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
from signals import Fibonacci_EMA_SignalGenerator
from signals import Fibonacci_EMA_BIAS_SignalGenerator
from signals import Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_SignalGenerator
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator
from signals import Fibonacci_EMA_TOP_ENTRANCE_SignalGenerator
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V2
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V3
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V4
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V5
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V6
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V7
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V8
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V9
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V10
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V12
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V13
from signals import Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V14
from signals import Fibonacci_EMA_CROSS_SignalGenerator


class Fibonacci_EMA_Strategy(BaseStrategy):
    """
    Fibonacci_EMA_Strategy
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        signal_generator = Fibonacci_EMA_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
class Fibonacci_EMA_BIAS_Strategy(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Strategy
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        signal_generator = Fibonacci_EMA_BIAS_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
class Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
    
class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
    
class Fibonacci_EMA_TOP_ENTRANCE_Strategy(BaseStrategy):
    """
    Fibonacci_EMA_TOP_ENTRANCE_Strategy
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_TOP_ENTRANCE_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_TOP_ENTRANCE_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
    
class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V2(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
    
class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V3(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V4(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
    
class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V5(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V6(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V7(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V8(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V9(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V10(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V12(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V13")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V13(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14(BaseStrategy):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V14")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V14(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)


class Fibonacci_EMA_CROSS_Strategy(BaseStrategy):
    """
    Fibonacci_EMA_CROSS_Strategy
    """
    def __init__(self):
        super().__init__(name="Fibonacci_EMA_CROSS_Strategy")
        self.params = {}

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        generate_signals
        """
        signal_generator = Fibonacci_EMA_CROSS_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)
