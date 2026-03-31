"""
网格交易策略

在预设的价格网格内进行低买高卖。
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
from constants import TRADE_METHOD
from signals import GridPositionManager


class Grid_Strategy(BaseStrategy):
    """
    网格交易策略
    
    策略说明：
    - 设置上涨卖出幅度和下跌买入幅度
    - 在网格触发时自动执行买入/卖出
    - 支持 T+1 限制和资金约束
    """
     
    def __init__(self, 
                 grid_up_pct: float = 0.03, 
                 grid_down_pct: float = 0.03, 
                 unit_type: str = 'share', 
                 trade_qty: int = 100, 
                 initial_cost: float = 0.0, 
                 initial_qty: int = 0, 
                 init_cash: float = 1000000,
                 base_position_cash: float = 800000):
        """
        初始化策略
        
        Args:
            grid_up_pct: 上涨卖出幅度 (如 0.03 表示 3%)
            grid_down_pct: 下跌买入幅度 (如 0.03 表示 3%)
            unit_type: 交易单位类型 ('share' 按股数, 'amount' 按金额)
            trade_qty: 每次交易的数量
            initial_cost: 底仓成本价
            initial_qty: 底仓数量
            init_cash: 初始资金
            base_position_cash: 底仓资金
        """
        # 参数自检
        if grid_up_pct < 0 or grid_down_pct < 0:
            raise ValueError(
                f"grid_up_pct/grid_down_pct 必须为正数比例(如 0.03)，"
                f"当前: up={grid_up_pct}, down={grid_down_pct}。"
            )
        if grid_up_pct >= 1 or grid_down_pct >= 1:
            raise ValueError(
                f"grid_up_pct/grid_down_pct 看起来像是百分数而非比例，"
                f"当前: up={grid_up_pct}, down={grid_down_pct}。"
            )
        
        name = f"Grid_Strategy_{grid_up_pct}_{grid_down_pct}"
        super().__init__(name=name)

        self.params = {
            "grid_up": grid_up_pct,
            "grid_down": grid_down_pct,
            "unit_type": unit_type,
            "trade_qty": trade_qty,
            "initial_cost": initial_cost,
            "initial_qty": initial_qty,
            "init_cash": init_cash,
            "base_position_cash": base_position_cash
        }

    def generate_signals(self, data_dict: Dict[str, pd.DataFrame]) -> Tuple[Any, Dict[str, Any]]:
        """
        生成交易信号
        
        Args:
            data_dict: OHLCV 数据字典
            
        Returns:
            (TRADE_METHOD, signal_dict)
        """
        manager = GridPositionManager(
            self.params, 
            total_cash=self.params['init_cash'], 
            base_position_cash=self.params['base_position_cash']
        )
        result = manager.run_simulation(data_dict, data_dict['close'].index)
        return TRADE_METHOD.SIZE_AND_PRICE, result

