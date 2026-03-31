"""
BIAS + MACD 金钻策略

日线条件：
- DIFF_BIAS > 0 (BIAS55 - BIAS20 > 0)
- BIAS20 上穿 0 轴

周线条件：
- CCI > 100
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
from signals import BIAS_MACD_JINZUAN_SignalGenerator


class BIAS_MACD_JINZUAN(BaseStrategy):
    """
    BIAS + MACD 金钻策略
    
    策略说明：
    {----------------日线级别计算----------------}
    { 日均价 }
    DAVERAGE := IF(VOL > 0, AMOUNT / (VOL * 100), CLOSE);

    { 均线 }
    MA20  := MA(DAVERAGE, 20);
    MA55  := MA(DAVERAGE, 55);

    { BIAS 计算 }
    BIAS20 := (DAVERAGE - MA20) / MA20 * 100;
    BIAS55 := (DAVERAGE - MA55) / MA55 * 100;

    { 乖离差值 }
    DIFF_BIAS := BIAS55 - BIAS20;

    { 日线条件1：DIFF_BIAS > 0 }
    COND1 := DIFF_BIAS > 0;

    { 日线条件2：BIAS20 上穿 0 轴 }
    COND2 := CROSS(BIAS20, 0);


    {----------------周线级别引用----------------}
    { 引用系统CCI指标的周线数值 }
    周CCI := "CCI.CCI#WEEK";

    { 周线条件：CCI > 100 }
    WEEK_COND : 周CCI > 100;


    {----------------最终选股输出----------------}
    { 逻辑：日线满足两个条件，且当周的CCI大于100 }
    XG: COND1 AND COND2 AND WEEK_COND;
    """
    
    def __init__(self):
        super().__init__(name="BIAS_MACD_JINZUAN")
        self.params = {}
        
    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        signal_generator = BIAS_MACD_JINZUAN_SignalGenerator(params=self.params)
        return signal_generator.run_simulation(data_dict, data_dict['close'].index)

