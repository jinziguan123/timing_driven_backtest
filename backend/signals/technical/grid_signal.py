"""
网格交易信号生成器
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

import numpy as np
import pandas as pd
from typing import Dict, Any

from signals._numba import numba_grid_simulator


class GridPositionManager:
    """
    网格交易仓位管理器
    
    实现网格交易策略的信号生成，支持：
    - 跨网格（逐格）交易
    - 跳空（gap）处理：高开/低开时用开盘价成交
    - T+1 交易限制
    - 资金约束检查
    """
    
    def __init__(self, grid_params: Dict[str, Any], total_cash: float = 1000000, 
                 base_position_cash: float = 800000, fees: float = 0.0003):
        """
        初始化网格仓位管理器
        
        Args:
            grid_params: 网格参数字典，包含:
                - grid_up: 上涨卖出幅度 (如 0.03 表示 3%)
                - grid_down: 下跌买入幅度 (如 0.03 表示 3%)
                - trade_qty: 每次交易数量
            total_cash: 总资金
            base_position_cash: 底仓资金
            fees: 手续费率
        """
        self.params = grid_params
        
        # 参数自检：避免出现反直觉情况
        grid_up = float(self.params.get('grid_up', 0.0))
        grid_down = float(self.params.get('grid_down', 0.0))
        
        if grid_up < 0 or grid_down < 0:
            raise ValueError(
                f"grid_up/grid_down 必须为正数比例(如 0.03)，当前: up={grid_up}, down={grid_down}。"
                "注意：不要把'下跌'理解成负号。"
            )
        if grid_up >= 1 or grid_down >= 1:
            raise ValueError(
                f"grid_up/grid_down 看起来像是百分数而非比例(如 3 而不是 0.03)，"
                f"当前: up={grid_up}, down={grid_down}。"
            )
        
        self.total_cash = total_cash
        self.base_position_cash = base_position_cash
        self.fees = fees

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Dict[str, pd.DataFrame]:
        """
        运行网格交易模拟
        
        返回给引擎的结果：
        - size: 下单数量矩阵 (+为买, -为卖)
        - price: 成交价矩阵

        说明：为了兼容 vectorbt 的单 bar 单价撮合，本实现会把同一 bar 内触发的多笔网格单
        在输出上合并成"一个净下单量"。
        
        Args:
            data_dict: OHLCV 数据字典
            time_index: 时间索引
            
        Returns:
            包含 'size' 和 'price' DataFrame 的字典
        """
        close_arr = data_dict['close'].to_numpy(dtype=np.float32)
        open_arr = data_dict.get('open', data_dict['close']).to_numpy(dtype=np.float32)
        high_arr = data_dict.get('high', data_dict['close']).to_numpy(dtype=np.float32)
        low_arr = data_dict.get('low', data_dict['close']).to_numpy(dtype=np.float32)

        dates = (time_index.year * 10000 + time_index.month * 100 + time_index.day).values.astype(np.int64)

        size_matrix, price_matrix = numba_grid_simulator(
            open_arr,
            close_arr,
            high_arr,
            low_arr,
            dates,
            self.total_cash,
            self.base_position_cash,
            self.fees,
            self.params['grid_up'],
            self.params['grid_down'],
            self.params['trade_qty'],
        )

        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)
        price_df = pd.DataFrame(price_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)
        
        return {"size": size_df, "price": price_df}

