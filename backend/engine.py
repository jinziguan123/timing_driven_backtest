"""
回测引擎
负责协调数据、Manager和Vectorbt
"""

import inspect
import vectorbt as vbt
import numpy as np
import pandas as pd
from typing import Dict, Optional, Any
from analysis import StrategyVisualizer
from result_saver import ResultSaver
from constants import TRADE_METHOD


class BacktestEngine:
    def __init__(self,
                 data_dict,
                 init_cash=100000.0,
                 fees=0.0003,
                 slippage=0.001,
                 max_positions=20,
                 tp_pct=0.10,
                 sl_pct=0.07,
                 position_size_pct=0.05,
                 random_seed=42
    ):

        self.data_dict = data_dict
        self.init_cash = init_cash
        self.fees = fees
        self.slippage = slippage
        self.portfolio = None
        self.visualizer = None
        self.strategy_name: Optional[str] = None
        self.size_matrix = None
        self.processed_entries = None
        self.processed_exits = None
        self.debug_pos_data = None

    def run(self, strategy_instance):
        # 1. 运行策略+仿真 (这一步完成了所有逻辑：信号、T+1、资金检查)
        print(">>> 正在运行 Numba 仿真核心...")
        self.strategy_name = getattr(strategy_instance, "name", strategy_instance.__class__.__name__)

        # 兼容 generate_signals(data_dict) 与 generate_signals(data_dict, init_cash)
        trade_method, signal_output = strategy_instance.generate_signals(self.data_dict)

        # 2. 根据返回类型选择 from_orders 或 from_signals
        if trade_method == TRADE_METHOD.BUY_AND_SELL_SIGNALS:
            # entries/exits 模式
            self.processed_entries = signal_output['entries']
            self.processed_exits = signal_output['exits']
            print(">>> 采用信号模式构建组合 (from_signals)...")
            self.portfolio = vbt.Portfolio.from_signals(
                close=self.data_dict['close'],
                entries=self.processed_entries,
                exits=self.processed_exits,
                init_cash=self.init_cash,
                fees=self.fees,
                slippage=self.slippage,
                freq='1min',
                direction='longonly'
            )
            self.size_matrix = None
        elif trade_method == TRADE_METHOD.SIZE_AND_PRICE:
            # size 矩阵模式（支持 {size, price}）
            self.size_matrix = signal_output['size']
            price_matrix = signal_output['price']

            # 将 size 矩阵中的 0 替换为 nan (vbt 规范)
            # vbt 中，size=0 可能会被误解，最好用 NaN 表示无操作
            if hasattr(self.size_matrix, "replace"):
                self.size_matrix = self.size_matrix.replace(0, np.nan)

            print(">>> 正在进行 Vectorbt 会计核算...")
            self.portfolio = vbt.Portfolio.from_orders(
                close=self.data_dict['close'],
                size=self.size_matrix,
                price=price_matrix,
                size_type='amount',
                init_cash=self.init_cash,
                fees=self.fees,
                slippage=self.slippage,
                freq='1min',
                cash_sharing=False,
                group_by=False
            )

        if self.portfolio is None:
            raise ValueError("回测失败，未能创建有效的 portfolio 对象")
        # 3. 附加调试数据
        self.debug_pos_data = self.portfolio.assets()
        self.visualizer = StrategyVisualizer(self.portfolio, self.data_dict, self.debug_pos_data)

    def analyze(self, specific_stock=None):
        if self.portfolio is None:
            return
        print("\n=== 回测统计报告 ===")
        self.visualizer.show_stats(specific_stock)

        if specific_stock:
            self.visualizer.plot_trade_record(specific_stock)

    def save_result(self, strategy_name, additional_info=None):
        saver = ResultSaver()
        return saver.save_backtest_result(self, strategy_name, additional_info)

    def get_batch_results(self) -> Optional[Dict[str, Any]]:
        """
        提取用于分批处理的轻量级结果。
        自动过滤掉没有产生交易的股票。
        """
        if self.portfolio is None:
            return None

        # 1. 识别活跃股票 (Total Trades > 0)
        trades_count = self.portfolio.trades.count()
        # trades_count 是一个 Series，索引是 columns (股票代码)
        active_symbols = trades_count[trades_count > 0].index.tolist()
        
        if not active_symbols:
            print(">>> 当前批次无交易产生。")
            return None

        print(f">>> 当前批次提取 {len(active_symbols)} 只活跃股票数据...")

        # 2. 提取数据 (只保留活跃股票)
        # 遍历所有活跃股票，提取stats
        active_stats = {}
        for symbol in active_symbols:
            stats = self.portfolio[symbol].stats()
            active_stats[symbol] = stats
        
        # Equity (DataFrame: index=time, columns=stocks)
        # value() 返回每只股票的资金曲线
        all_equity = self.portfolio.value()
        active_equity = all_equity[active_symbols]
        
        # Orders & Trades (DataFrame)
        # records_readable 是一个长表 (DataFrame)
        all_orders = self.portfolio.orders.records_readable
        active_orders = all_orders[all_orders['Column'].isin(active_symbols)].copy()
        
        all_trades = self.portfolio.trades.records_readable
        active_trades = all_trades[all_trades['Column'].isin(active_symbols)].copy()
        
        # Positions (DataFrame: index=time, columns=stocks)
        if self.debug_pos_data is not None:
             active_positions = self.debug_pos_data[active_symbols]
        else:
             active_positions = pd.DataFrame()
             
        return {
            'stats_dict': active_stats,
            'equity_df': active_equity,
            'orders_df': active_orders,
            'trades_df': active_trades,
            'positions_df': active_positions,
            'symbols': active_symbols,
            'init_cash': self.init_cash # 用于后续聚合计算
        }
