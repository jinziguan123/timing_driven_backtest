"""
BIAS + EXPMA 策略

日线条件：
- EMA 三线多头排列 (EMA13 > EMA34 > EMA55)
- BIAS 策略条件

周线条件：
- EXPMA(14) > EXPMA(55)
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from typing import Dict, Tuple, Any
import pandas as pd

from core.base_strategy import BaseStrategy
from signals import BIAS_EXPMA_SignalGenerator


class BIAS_EXPMA_Strategy(BaseStrategy):
    """
    BIAS + EXPMA 策略
    
    策略说明：
    日线条件：
    { === 条件一：EMA 三线多头排列 === }
    EMA13 := EMA(CLOSE, 13);
    EMA34 := EMA(CLOSE, 34);
    EMA55 := EMA(CLOSE, 55);
    COND_EMA := EMA13 > EMA34 AND EMA34 > EMA55;

    { === 条件二：基于日均价的 BIAS 策略 === }
    DAVERAGE := IF(VOL > 0, AMOUNT / (VOL * 100), CLOSE);
    MA20  := MA(DAVERAGE, 20);
    MA55  := MA(DAVERAGE, 55);
    MA120 := MA(DAVERAGE, 120);

    BIAS20  := (DAVERAGE - MA20) / MA20 * 100;
    BIAS55  := (DAVERAGE - MA55) / MA55 * 100;
    BIAS120 := (DAVERAGE - MA120) / MA120 * 100;

    DIFF_BIAS  := BIAS55 - BIAS20;
    DIFF_BIAS2 := BIAS120 - BIAS55;

    COND1 := CROSS(BIAS20, 0);        { BIAS20 上穿 0 }
    COND2 := DIFF_BIAS > 0;           { BIAS55 > BIAS20 }
    COND3 := DIFF_BIAS2 < 10;         { 长中期乖离差小于10 }

    COND_BIAS := COND1 AND COND2 AND COND3;

    { === 综合条件：同时满足 EMA 多头 + BIAS 信号 === }
    COND_EMA AND COND_BIAS;

    周线条件：
    EXPMA(CLOSE,14) > EXPMA(CLOSE,55);
    """
    
    def __init__(self):
        super().__init__(name="BIAS_EXPMA_Strategy")
        self.params = {}
        
    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        signal_generator = BIAS_EXPMA_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)

