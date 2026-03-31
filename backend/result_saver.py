"""
回测结果持久化模块

设计目标（对应你的 3 点诉求）：
1) 不再落盘分钟线/日线等行情数据（行情可复用，应从 data_manager 动态读取）。
2) stats/metadata 支持按股票维度持久化（不再只是一份“全局统计”）。
3) 不再落盘买卖信号（entries/exits/size_matrix），只保留用户真正关心的交易记录与必要状态。

仍然落盘：
- stats.json: 全局 stats + stats_by_symbol
- metadata.json: 回测基础信息（stock_list、时间范围等）
- equity.parquet: 组合净值（按列为股票）
- orders.parquet / trades.parquet: 交易记录
- positions.parquet: 绝对持仓（用于分钟线叠加持仓、计算当日累计盈亏等）
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from functools import lru_cache

import numpy as np
import pandas as pd
import vectorbt as vbt # 引入vbt用于计算汇总指标

from data_manager import load_stock_minutes


def _jsonify_scalar(v: Any) -> Any:
    """把 numpy/pandas 标量安全地转成 JSON 可序列化。"""
    if v is None:
        return None
    if isinstance(v, (np.integer, int)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        fv = float(v)
        if np.isnan(fv) or np.isinf(fv):
            return None
        return fv
    # pandas Timestamp / Timedelta
    if hasattr(v, 'isoformat'):
        try:
            return v.isoformat()
        except Exception:
            pass
    return str(v)


def _series_to_dict(s: pd.Series) -> Dict[str, Any]:
    return {str(k): _jsonify_scalar(v) for k, v in s.items()}


def _coerce_time_range_from_df_index(df: pd.DataFrame) -> Optional[Tuple[str, str]]:
    if df is None or df.empty:
        return None
    idx = df.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            idx = pd.to_datetime(idx)
        except Exception:
            return None
    if len(idx) == 0:
        return None
    start = idx.min().to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
    end = idx.max().to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
    return start, end


class ResultSaver:
    """回测结果保存和加载器 - 优化：添加缓存机制减少重复加载"""

    def __init__(self, save_dir: str = "backtest_results", enable_cache: bool = True):
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(exist_ok=True)
        self.enable_cache = enable_cache

        # 内存缓存：{ (result_id, symbol, ...): DataFrame }
        self._cache: Dict[tuple, Any] = {}
        self._cache_timestamps: Dict[tuple, float] = {}

    def _load_result_metadata(self, result_id: str) -> Dict[str, Any]:
        result_dir = self.save_dir / result_id
        meta_path = result_dir / "metadata.json"
        if not meta_path.exists():
            return {}
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _resolve_market_data_config(self, result_id: str) -> Dict[str, str]:
        metadata = self._load_result_metadata(result_id)
        source = metadata.get('bar_source') or 'dat'
        adjust = metadata.get('adjust_mode') or 'none'
        return {
            'source': str(source),
            'adjust': str(adjust),
        }
        self._cache_ttl = 300  # 缓存5分钟
        self._cache_max_size = 100  # 最多缓存100个条目

    def _get_cache_key(self, result_id: str, *args) -> tuple:
        """生成缓存键"""
        return (result_id,) + tuple[str | None, ...](str(arg) if arg is not None else None for arg in args)

    def _get_from_cache(self, key: tuple) -> Optional[Any]:
        """从缓存获取数据"""
        if not self.enable_cache:
            return None
        if key not in self._cache:
            return None

        # 检查缓存是否过期
        import time
        if time.time() - self._cache_timestamps[key] > self._cache_ttl:
            self._cache.pop(key, None)
            self._cache_timestamps.pop(key, None)
            return None

        return self._cache[key]

    def _set_cache(self, key: tuple, value: Any):
        """设置缓存"""
        if not self.enable_cache:
            return

        import time
        # 如果缓存已满，清理最旧的条目
        if len(self._cache) >= self._cache_max_size:
            oldest_key = min(self._cache_timestamps.items(), key=lambda x: x[1])[0]
            self._cache.pop(oldest_key, None)
            self._cache_timestamps.pop(oldest_key, None)

        self._cache[key] = value
        self._cache_timestamps[key] = time.time()

    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()
        self._cache_timestamps.clear()

    def filter_df_by_date_range(
        self, 
        df: pd.DataFrame, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        根据日期范围过滤 DataFrame
        
        Args:
            df: 需要过滤的 DataFrame，index 应为 DatetimeIndex
            start_date: 开始日期，格式 YYYY-MM-DD
            end_date: 结束日期，格式 YYYY-MM-DD
            
        Returns:
            过滤后的 DataFrame
        """
        if df is None or df.empty:
            return df
        
        if not start_date and not end_date:
            return df
        
        # 确保索引是 DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df = df.copy()
                df.index = pd.to_datetime(df.index)
            except Exception:
                return df
        
        # 应用过滤
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df = df[df.index >= start_dt]
        
        if end_date:
            end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            df = df[df.index <= end_dt]
        
        return df
    
    def get_filtered_stats(
        self,
        result_id: str,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        根据时间区间重新计算统计数据
        
        Args:
            result_id: 回测结果ID
            symbol: 股票代码（可选）
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            重新计算的统计数据，如果无法计算则返回 None
        """
        if not start_date and not end_date:
            return None
        
        result_dir = self.save_dir / result_id
        
        try:
            # 加载并过滤权益数据
            if symbol:
                equity_df = self.load_stock_daily_equity(result_id, symbol)
                if not equity_df.empty and symbol in equity_df.columns:
                    equity_series = equity_df[symbol]
                else:
                    return None
            else:
                equity_df = self.load_total_daily_equity(result_id)
                if not equity_df.empty and 'total_equity' in equity_df.columns:
                    equity_series = equity_df['total_equity']
                else:
                    return None
            
            # 应用时间过滤
            equity_series = self.filter_df_by_date_range(equity_series.to_frame(), start_date, end_date)
            if equity_series.empty:
                return None
            equity_series = equity_series.iloc[:, 0]
            
            # 加载并过滤交易数据
            trades_df = self.load_trades(result_id, symbol, start_date, end_date)
            
            # 计算统计指标
            stats = {}
            
            # 时间范围
            stats['Start'] = str(equity_series.index[0])
            stats['End'] = str(equity_series.index[-1])
            stats['Period'] = str(equity_series.index[-1] - equity_series.index[0])
            
            # 收益相关
            start_value = float(equity_series.iloc[0])
            end_value = float(equity_series.iloc[-1])
            stats['Start Value'] = start_value
            stats['End Value'] = end_value
            total_return = (end_value - start_value) / start_value * 100 if start_value > 0 else 0.0
            stats['Total Return [%]'] = total_return
            
            # 最大回撤
            rolling_max = equity_series.cummax()
            drawdown_series = (equity_series - rolling_max) / rolling_max
            max_dd = abs(drawdown_series.min() * 100)
            stats['Max Drawdown [%]'] = max_dd
            
            # 日收益率和夏普比率
            daily_returns = equity_series.pct_change().dropna()
            if len(daily_returns) > 1 and daily_returns.std() != 0:
                sharpe = daily_returns.mean() / daily_returns.std() * np.sqrt(252)
                stats['Sharpe Ratio'] = float(sharpe)
            else:
                stats['Sharpe Ratio'] = 0.0
            
            # 交易统计
            if not trades_df.empty:
                closed_trades = trades_df[trades_df['Status'] == 'Closed'] if 'Status' in trades_df.columns else trades_df
                stats['Total Trades'] = len(trades_df)
                stats['Total Closed Trades'] = len(closed_trades)
                
                if not closed_trades.empty and 'PnL' in closed_trades.columns:
                    winning = closed_trades[closed_trades['PnL'] > 0]
                    stats['Win Rate [%]'] = len(winning) / len(closed_trades) * 100 if len(closed_trades) > 0 else 0.0
            else:
                stats['Total Trades'] = 0
                stats['Win Rate [%]'] = 0.0
            
            return stats
            
        except Exception as e:
            print(f"计算过滤后统计数据时出错: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _compute_global_stats_from_batch(
        self,
        stats_dict: Dict[str, Any],
        full_equity: pd.DataFrame,
        full_trades: pd.DataFrame,
        init_cash: float,
        result_dir: Path
    ) -> Dict[str, Any]:
        """
        基于"分步计算 + 最终聚合"策略计算全局统计指标。
        
        核心思路：
        1. 每批次回测后，提取该批次所有股票的关键指标（stats_dict）
        2. 基于 stats_dict 聚合计算全局指标，而非依赖完整的 equity DataFrame
        3. 准确计算：最大回撤、胜率、总收益率、夏普比率
        
        Args:
            stats_dict: 每只股票的统计指标字典 {symbol: {stat_name: value, ...}, ...}
            full_equity: 合并后的权益 DataFrame（用于时间范围和落盘）
            full_trades: 合并后的交易记录
            init_cash: 单只股票的初始资金
            result_dir: 结果保存目录
            
        Returns:
            全局统计指标字典
        """
        global_stats_summary = {}
        
        try:
            # ================================================================
            # A. 从 stats_dict 提取并聚合每只股票的关键指标
            # ================================================================
            stock_stats_list = []
            for symbol, stat_val in stats_dict.items():
                stat_dict = stat_val if isinstance(stat_val, dict) else _series_to_dict(stat_val)
                stat_dict['symbol'] = symbol
                stock_stats_list.append(stat_dict)
            
            if not stock_stats_list:
                raise ValueError("stats_dict 为空，无法计算全局统计")
            
            # 转换为 DataFrame 便于聚合计算
            stock_stats_df = pd.DataFrame(stock_stats_list)
            total_stocks = len(stock_stats_df)
            
            # ================================================================
            # B. 计算总收益率 (Total Return)
            # 方法：基于每只股票的 Start Value 和 End Value 加权计算
            # ================================================================
            if 'Start Value' in stock_stats_df.columns and 'End Value' in stock_stats_df.columns:
                total_start_value = stock_stats_df['Start Value'].sum()
                total_end_value = stock_stats_df['End Value'].sum()
            else:
                # 兜底：使用 init_cash * 股票数量 作为起始值
                total_start_value = init_cash * total_stocks
                # 从 Total Return [%] 反推 End Value
                if 'Total Return [%]' in stock_stats_df.columns:
                    stock_stats_df['_end_value_calc'] = init_cash * (1 + stock_stats_df['Total Return [%]'] / 100)
                    total_end_value = stock_stats_df['_end_value_calc'].sum()
                else:
                    total_end_value = total_start_value
            
            # 计算组合总收益率
            total_return_pct = (total_end_value - total_start_value) / total_start_value * 100 if total_start_value > 0 else 0.0
            
            # ================================================================
            # C. 计算最大回撤 (Max Drawdown)
            # 方法1：基于组合权益曲线计算（最准确）
            # 方法2：从各股票回撤中取最差值（备用）
            # ================================================================
            max_dd_pct = 0.0
            avg_dd_pct = 0.0
            
            # 尝试方法1：基于组合权益曲线
            try:
                total_equity_series = full_equity.sum(axis=1)
                if not isinstance(total_equity_series.index, pd.DatetimeIndex):
                    total_equity_series.index = pd.to_datetime(total_equity_series.index)
                
                # 落盘每日权益数据
                daily_total_equity = total_equity_series.resample('1D').last().dropna()
                daily_total_equity.name = "total_equity"
                daily_total_equity.to_frame().to_parquet(result_dir / "daily_equity.parquet")
                
                daily_stock_equity = full_equity.resample('1D').last().dropna(how='all')
                daily_stock_equity.to_parquet(result_dir / "daily_stock_equity.parquet")
                
                # 基于组合权益计算最大回撤
                rolling_max = total_equity_series.cummax()
                drawdown_series = (total_equity_series - rolling_max) / rolling_max
                max_dd_pct = abs(drawdown_series.min() * 100)  # 负数
                
            except Exception as e:
                print(f"基于权益曲线计算回撤失败，使用聚合方法: {e}")
            
            # 方法2：从 stats_dict 聚合
            if 'Max Drawdown [%]' in stock_stats_df.columns:
                dd_values = stock_stats_df['Max Drawdown [%]'].dropna()
                if len(dd_values) > 0:
                    # vbt 的 Max Drawdown [%] 通常是正数（幅度）
                    # 我们统一转为负数表示
                    dd_values_normalized = dd_values.apply(lambda x: -abs(x) if x > 0 else x)
                    avg_dd_pct = dd_values_normalized.mean()
                    worst_dd_from_stocks = dd_values_normalized.min()
                    
                    # 如果组合回撤计算失败，使用股票回撤的统计
                    if max_dd_pct == 0.0:
                        max_dd_pct = worst_dd_from_stocks
            
            # ================================================================
            # D. 计算胜率 (Win Rate)
            # 方法：基于所有交易记录统计
            # ================================================================
            trade_stats = {}
            win_rate_pct = 0.0
            
            if not full_trades.empty:
                closed_trades = full_trades[full_trades['Status'] == 'Closed']
                open_trades = full_trades[full_trades['Status'] == 'Open']
                
                total_trades = len(full_trades)
                total_closed = len(closed_trades)
                total_open = len(open_trades)
                
                # 胜率计算
                winning_trades = closed_trades[closed_trades['PnL'] > 0]
                losing_trades = closed_trades[closed_trades['PnL'] <= 0]
                win_rate_pct = (len(winning_trades) / total_closed * 100) if total_closed > 0 else 0.0
                
                # 其他交易统计
                if 'Return' in closed_trades.columns:
                    returns_arr = closed_trades['Return'].values
                    best_trade_pct = returns_arr.max() * 100 if len(returns_arr) > 0 else 0.0
                    worst_trade_pct = returns_arr.min() * 100 if len(returns_arr) > 0 else 0.0
                    avg_win_pct = winning_trades['Return'].mean() * 100 if not winning_trades.empty else 0.0
                    avg_loss_pct = losing_trades['Return'].mean() * 100 if not losing_trades.empty else 0.0
                else:
                    best_trade_pct = worst_trade_pct = avg_win_pct = avg_loss_pct = 0.0
                
                # 持仓时间
                if 'Exit Timestamp' in closed_trades.columns and 'Entry Timestamp' in closed_trades.columns:
                    durations = pd.to_datetime(closed_trades['Exit Timestamp']) - pd.to_datetime(closed_trades['Entry Timestamp'])
                    avg_win_duration = durations[closed_trades['PnL'] > 0].mean() if not winning_trades.empty else pd.Timedelta(0)
                    avg_loss_duration = durations[closed_trades['PnL'] <= 0].mean() if not losing_trades.empty else pd.Timedelta(0)
                else:
                    avg_win_duration = avg_loss_duration = pd.Timedelta(0)
                
                # 费用和盈利因子
                total_fees = full_trades['Fees'].sum() if 'Fees' in full_trades.columns else 0.0
                gross_profit = winning_trades['PnL'].sum() if not winning_trades.empty else 0.0
                gross_loss = abs(losing_trades['PnL'].sum()) if not losing_trades.empty else 0.0
                profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float('inf')
                expectancy = closed_trades['PnL'].mean() if total_closed > 0 else 0.0
                
                trade_stats = {
                    "Total Trades": total_trades,
                    "Total Closed Trades": total_closed,
                    "Total Open Trades": total_open,
                    "Open Trade PnL": open_trades['PnL'].sum() if not open_trades.empty else 0.0,
                    "Win Rate [%]": win_rate_pct,
                    "Best Trade [%]": best_trade_pct,
                    "Worst Trade [%]": worst_trade_pct,
                    "Avg Winning Trade [%]": avg_win_pct,
                    "Avg Losing Trade [%]": avg_loss_pct,
                    "Avg Winning Trade Duration": str(avg_win_duration),
                    "Avg Losing Trade Duration": str(avg_loss_duration),
                    "Total Fees Paid": total_fees,
                    "Profit Factor": profit_factor,
                    "Expectancy": expectancy,
                }
            else:
                # 如果没有交易记录，尝试从 stats_dict 聚合胜率
                if 'Win Rate [%]' in stock_stats_df.columns:
                    # 使用交易数量加权平均
                    if 'Total Trades' in stock_stats_df.columns:
                        weights = stock_stats_df['Total Trades'].fillna(0)
                        if weights.sum() > 0:
                            win_rate_pct = (stock_stats_df['Win Rate [%]'].fillna(0) * weights).sum() / weights.sum()
                    else:
                        win_rate_pct = stock_stats_df['Win Rate [%]'].mean()
                    trade_stats["Win Rate [%]"] = win_rate_pct
            
            # ================================================================
            # E. 计算夏普比率 (Sharpe Ratio)
            # 方法1：基于组合日收益率计算
            # 方法2：从各股票夏普比率加权平均（备用）
            # ================================================================
            sharpe_ratio = 0.0
            
            # 方法1：基于组合日收益率
            try:
                total_equity_series = full_equity.sum(axis=1)
                if not isinstance(total_equity_series.index, pd.DatetimeIndex):
                    total_equity_series.index = pd.to_datetime(total_equity_series.index)
                
                # 重采样到日线计算日收益率
                daily_equity = total_equity_series.resample('1D').last().dropna()
                daily_returns = daily_equity.pct_change().dropna()
                
                if len(daily_returns) > 1 and daily_returns.std() != 0:
                    # 年化因子：252 交易日
                    annual_factor = 252
                    sharpe_ratio = daily_returns.mean() / daily_returns.std() * np.sqrt(annual_factor)
                    
            except Exception as e:
                print(f"基于日收益计算夏普比率失败: {e}")
            
            # 方法2：从 stats_dict 聚合
            if sharpe_ratio == 0.0 and 'Sharpe Ratio' in stock_stats_df.columns:
                sharpe_values = stock_stats_df['Sharpe Ratio'].dropna()
                if len(sharpe_values) > 0:
                    # 使用资金加权平均
                    if 'End Value' in stock_stats_df.columns:
                        weights = stock_stats_df['End Value'].fillna(init_cash)
                        sharpe_ratio = (sharpe_values * weights.loc[sharpe_values.index]).sum() / weights.loc[sharpe_values.index].sum()
                    else:
                        sharpe_ratio = sharpe_values.mean()
            
            # ================================================================
            # F. 时间范围信息
            # ================================================================
            try:
                total_equity_series = full_equity.sum(axis=1)
                if not isinstance(total_equity_series.index, pd.DatetimeIndex):
                    total_equity_series.index = pd.to_datetime(total_equity_series.index)
                start_time = total_equity_series.index[0]
                end_time = total_equity_series.index[-1]
                period = end_time - start_time
            except:
                start_time = end_time = period = None
            
            # ================================================================
            # G. 聚合其他统计指标（从 stats_dict 平均）
            # ================================================================
            aggregated_from_stocks = {}
            aggregate_keys = [
                'Sortino Ratio', 'Calmar Ratio', 'Omega Ratio', 
                'Tail Ratio', 'Common Sense Ratio', 'Value at Risk'
            ]
            for key in aggregate_keys:
                if key in stock_stats_df.columns:
                    values = stock_stats_df[key].dropna()
                    if len(values) > 0:
                        aggregated_from_stocks[f"{key}"] = float(values.mean())
            
            # ================================================================
            # H. 汇总全局统计
            # ================================================================
            global_stats_summary = {
                # 基础信息
                "Start": str(start_time) if start_time else None,
                "End": str(end_time) if end_time else None,
                "Period": str(period) if period else None,
                "Start Value": float(total_start_value),
                "End Value": float(total_end_value),
                
                # 核心指标（精确计算）
                "Total Return [%]": float(total_return_pct),
                "Max Drawdown [%]": float(max_dd_pct),  # 负数表示
                "Avg Drawdown [%]": float(avg_dd_pct) if avg_dd_pct != 0 else None,
                "Sharpe Ratio": float(sharpe_ratio),
                
                # 回测规模
                "Total Stocks": total_stocks,
                "Benchmark Return [%]": 0.0,
                "Max Gross Exposure [%]": 100.0,
            }
            
            # 添加交易统计
            global_stats_summary.update(trade_stats)
            
            # 添加其他聚合指标
            global_stats_summary.update(aggregated_from_stocks)
            
            # ================================================================
            # I. 添加分布统计信息（供分析用）
            # ================================================================
            distribution_stats = {}
            if 'Total Return [%]' in stock_stats_df.columns:
                returns = stock_stats_df['Total Return [%]'].dropna()
                if len(returns) > 0:
                    distribution_stats['Return Distribution'] = {
                        'mean': float(returns.mean()),
                        'std': float(returns.std()),
                        'min': float(returns.min()),
                        'max': float(returns.max()),
                        'median': float(returns.median()),
                        'q25': float(returns.quantile(0.25)),
                        'q75': float(returns.quantile(0.75)),
                        'positive_count': int((returns > 0).sum()),
                        'negative_count': int((returns <= 0).sum()),
                    }
            
            if distribution_stats:
                global_stats_summary['Distribution Stats'] = distribution_stats
                
        except Exception as e:
            print(f"全局统计计算错误: {e}")
            import traceback
            traceback.print_exc()
            
            # Fallback: 简单统计
            try:
                total_equity_series = full_equity.sum(axis=1)
                total_return = (total_equity_series.iloc[-1] - total_equity_series.iloc[0]) / total_equity_series.iloc[0] * 100
                global_stats_summary = {
                    'Start': str(total_equity_series.index[0]),
                    'End': str(total_equity_series.index[-1]),
                    'Start Value': float(total_equity_series.iloc[0]),
                    'End Value': float(total_equity_series.iloc[-1]),
                    'Total Return [%]': float(total_return),
                    'Total Trades': len(full_trades),
                    'Total Stocks': len(full_equity.columns),
                }
            except:
                global_stats_summary = {'error': str(e)}
        
        return global_stats_summary

    def save_backtest_result(
        self,
        engine,
        strategy_name: str,
        additional_info: Optional[Dict[str, Any]] = None,
    ):
        """(旧接口) 保存单个 Engine 的结果"""
        # 简单包装，调用新的聚合保存接口
        results = engine.get_batch_results()
        if not results:
             print("警告：无有效交易数据可保存")
             return None
        
        # 构造成聚合格式
        return self.save_aggregated_result(
            aggregated_data={
                'stats_dict': results['stats_dict'],
                'equity_df_list': [results['equity_df']],
                'orders_df_list': [results['orders_df']],
                'trades_df_list': [results['trades_df']],
                'positions_df_list': [results['positions_df']],
                'symbols_list': results['symbols']
            },
            strategy_name=strategy_name,
            init_cash=engine.init_cash,
            additional_info=additional_info
        )

    def save_aggregated_result(
        self,
        aggregated_data: Dict[str, List[Any]],
        strategy_name: str,
        init_cash: float,
        additional_info: Optional[Dict[str, Any]] = None,
    ):
        """
        保存聚合后的回测结果
        
        aggregated_data 结构:
        {
            'stats_dict': {symbol1: stats_series/dict1, symbol2: stats_series/dict2, ...},
            'equity_df_list': [df1, df2, ...],
            ...
        }
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_id = f"{strategy_name}_{timestamp}"
        result_dir = self.save_dir / result_id
        result_dir.mkdir(exist_ok=True)

        print(f"正在保存聚合回测结果到: {result_dir}")
        
        # 1. 合并数据
        # Equity: 列合并 (axis=1)
        equity_dfs = aggregated_data.get('equity_df_list', [])
        if not equity_dfs:
            print("错误：没有权益数据")
            return None
        full_equity = pd.concat(equity_dfs, axis=1)
        
        # 修正：处理不同股票的时间对齐问题
        # 对于空值（未上市或停牌期间），应视为持有初始资金（未投入）或沿用上一个有效值
        full_equity = full_equity.ffill().fillna(float(init_cash))
        
        # Stats: 合并 stats_dict_list 到 stats_by_symbol
        stats_dict = aggregated_data.get('stats_dict', {})
        # 将每一个value都从series转化为dict
        for sym, stat_val in stats_dict.items():
            if isinstance(stat_val, pd.Series):
                stats_dict[sym] = _series_to_dict(stat_val)

        # Orders / Trades: 行合并 (axis=0)
        orders_dfs = aggregated_data.get('orders_df_list', [])
        full_orders = pd.concat(orders_dfs, axis=0, ignore_index=True) if orders_dfs else pd.DataFrame()
        
        trades_dfs = aggregated_data.get('trades_df_list', [])
        full_trades = pd.concat(trades_dfs, axis=0, ignore_index=True) if trades_dfs else pd.DataFrame()
        
        positions_dfs = aggregated_data.get('positions_df_list', [])
        full_positions = pd.concat(positions_dfs, axis=1) if positions_dfs else pd.DataFrame()
        
        symbols = full_equity.columns.tolist()

        # 2. 计算全局 Stats (Aggregation)
        # ============================================================================
        # 采用"分步计算 + 最终聚合"策略：
        # 核心思路：基于 stats_dict（每只股票的统计指标）进行聚合计算，
        # 而非依赖完整的 equity DataFrame，以避免大规模回测时内存溢出。
        # ============================================================================
        global_stats_summary = self._compute_global_stats_from_batch(
            stats_dict=stats_dict,
            full_equity=full_equity,
            full_trades=full_trades,
            init_cash=init_cash,
            result_dir=result_dir
        )

        stats_dict: Dict[str, Any] = {
            'strategy_name': strategy_name,
            'timestamp': timestamp,
            'init_cash': float(init_cash), # 单个股票的初始资金
            'stats': global_stats_summary, # 全局详细统计
            'stats_by_symbol': stats_dict,
        }
        if additional_info:
            stats_dict.update(additional_info)

        with open(result_dir / "stats.json", 'w', encoding='utf-8') as f:
            json.dump(stats_dict, f, ensure_ascii=False, indent=2)

        # 3. 落盘 Parquet
        full_equity.to_parquet(result_dir / "equity.parquet")
        if not full_orders.empty:
            full_orders.to_parquet(result_dir / "orders.parquet")
        if not full_trades.empty:
            full_trades.to_parquet(result_dir / "trades.parquet")
        if not full_positions.empty:
             full_positions.to_parquet(result_dir / "positions.parquet")
             
        # 4. Metadata
        time_range = _coerce_time_range_from_df_index(full_equity)
        start_time, end_time = (time_range or (None, None))
        
        metadata: Dict[str, Any] = {
            'result_id': result_id,
            'strategy_name': strategy_name,
            'timestamp': timestamp,
            'data_shape': {
                'time_points': len(full_equity.index),
                'stocks': len(symbols),
            },
            'stock_list': symbols,
            'start_time': start_time,
            'end_time': end_time,
        }
        if additional_info:
            for key in ['bar_source', 'adjust_mode', 'adjust_factor_source']:
                if key in additional_info:
                    metadata[key] = additional_info[key]
        
        with open(result_dir / "metadata.json", 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)

        print(f"回测结果已保存到: {result_dir}")
        return result_id

    def load_total_daily_equity(self, result_id: str) -> pd.DataFrame:
        """加载每日总资产曲线 (Portfolio) - 优化：添加缓存"""
        # 检查缓存
        cache_key = self._get_cache_key(result_id, "total_daily_equity")
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data.copy()

        result_dir = self.save_dir / result_id
        path = result_dir / "daily_equity.parquet"
        if not path.exists():
             # 兼容旧数据：尝试从 equity.parquet 聚合
             equity_path = result_dir / "equity.parquet"
             if equity_path.exists():
                 try:
                     full = pd.read_parquet(equity_path)
                     # 简单求和并重采样
                     total = full.sum(axis=1).resample('1D').last().dropna()
                     result = total.to_frame(name="total_equity")
                     self._set_cache(cache_key, result)
                     return result
                 except Exception:
                     return pd.DataFrame()
             return pd.DataFrame()

        result = pd.read_parquet(path)
        self._set_cache(cache_key, result)
        return result

    def load_stock_daily_equity(self, result_id: str, symbol: Optional[str] = None) -> pd.DataFrame:
        """加载每日个股资产曲线 - 优化：使用列裁剪 + 缓存"""
        # 检查缓存
        cache_key = self._get_cache_key(result_id, "stock_daily_equity", symbol)
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data.copy()  # 返回副本避免修改缓存

        result_dir = self.save_dir / result_id
        path = result_dir / "daily_stock_equity.parquet"
        if not path.exists():
             # 兼容旧数据：尝试从 equity.parquet 重采样
             equity_path = result_dir / "equity.parquet"
             if equity_path.exists():
                 try:
                     # 优化：只读取指定列
                     columns = [symbol] if symbol else None
                     full = pd.read_parquet(equity_path, columns=columns)
                     if symbol:
                        if symbol in full.columns:
                            result = full[[symbol]].resample('1D').last().dropna()
                        else:
                            result = pd.DataFrame()
                     else:
                        result = full.resample('1D').last().dropna(how='all')
                     self._set_cache(cache_key, result)
                     return result
                 except Exception:
                     return pd.DataFrame()
             return pd.DataFrame()

        # 优化：指定列名，只读取该股票的列（不加载其他股票数据）
        if symbol:
            try:
                df = pd.read_parquet(path, columns=[symbol])
                if symbol in df.columns:
                    result = df[[symbol]].dropna()
                else:
                    result = pd.DataFrame()
            except Exception:
                return pd.DataFrame()
        else:
            result = pd.read_parquet(path)

        self._set_cache(cache_key, result)
        return result

    def load_backtest_result(self, result_id: str) -> Dict[str, Any]:
        result_dir = self.save_dir / result_id
        if not result_dir.exists():
            raise FileNotFoundError(f"回测结果不存在: {result_dir}")

        result: Dict[str, Any] = {}

        with open(result_dir / "stats.json", 'r', encoding='utf-8') as f:
            result['stats'] = json.load(f)

        with open(result_dir / "metadata.json", 'r', encoding='utf-8') as f:
            result['metadata'] = json.load(f)

        equity_path = result_dir / "equity.parquet"
        result['equity'] = pd.read_parquet(equity_path) if equity_path.exists() else pd.DataFrame()

        orders_path = result_dir / "orders.parquet"
        result['orders'] = pd.read_parquet(orders_path) if orders_path.exists() else pd.DataFrame()

        trades_path = result_dir / "trades.parquet"
        if trades_path.exists():
            result['trades'] = pd.read_parquet(trades_path)

        positions_path = result_dir / "positions.parquet"
        result['positions'] = pd.read_parquet(positions_path) if positions_path.exists() else None

        # 兼容老结果：如果历史版本仍保存了日线/分钟线/信号文件，这里不主动加载
        return result

    def _get_time_range_for_result(self, result_id: str) -> Tuple[Optional[str], Optional[str]]:
        """尽量从 metadata.json / equity.parquet 推导回测时间范围"""
        result_dir = self.save_dir / result_id
        meta_path = result_dir / 'metadata.json'
        if meta_path.exists():
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                st = meta.get('start_time')
                et = meta.get('end_time')
                if st and et:
                    return st, et
            except Exception:
                pass

        equity_path = result_dir / 'equity.parquet'
        if equity_path.exists():
            try:
                eq = pd.read_parquet(equity_path)
                tr = _coerce_time_range_from_df_index(eq)
                if tr:
                    return tr
            except Exception:
                pass

        return None, None

    def select_stats_for_symbol(self, stats_json: Dict[str, Any], symbol: Optional[str]) -> Dict[str, Any]:
        """给 API 用：从 stats.json 中挑选对应 symbol 的 stats（不改变外层结构）。"""
        if not symbol:
            return stats_json
        by_sym = stats_json.get('stats_by_symbol') or {}
        if isinstance(by_sym, dict) and symbol in by_sym:
            # 维持前端期望：stats 字段仍是一个 dict
            out = dict(stats_json)
            out['stats'] = by_sym[symbol]
            return out
        
        # 如果指定了 symbol 但没找到对应的 stats (例如无交易)，
        # 不应该返回全局 stats，而应该返回空，避免误导
        if symbol:
            out = dict(stats_json)
            out['stats'] = {} 
            return out
            
        return stats_json

    def load_daily_ohlcv(self, result_id: str, symbol: Optional[str] = None) -> pd.DataFrame:
        """动态日线：优先从 data_manager 拉分钟线，然后聚合成日线。

        注意：为避免巨大开销，这里强烈建议传 symbol；否则返回空。
        """
        if not symbol:
            return pd.DataFrame()

        # 兼容老结果：如果历史版本落盘了 daily_ohlcv.parquet，优先用
        result_dir = self.save_dir / result_id
        daily_path = result_dir / "daily_ohlcv.parquet"
        if daily_path.exists():
            try:
                df = pd.read_parquet(daily_path)
                # 老格式可能是 MultiIndex 列（field, symbol）
                try:
                    df = df.xs(symbol, level=1, axis=1)
                except Exception:
                    pass
                return df
            except Exception:
                pass

        start_time, end_time = self._get_time_range_for_result(result_id)
        market_data_config = self._resolve_market_data_config(result_id)
        minutes = load_stock_minutes(
            symbol,
            start_time,
            end_time,
            fields=['open', 'high', 'low', 'close', 'volume', 'amount'],
            source=market_data_config['source'],
            adjust=market_data_config['adjust'],
        )
        if minutes.empty:
            return pd.DataFrame()

        # 聚合日线
        daily = pd.DataFrame(index=minutes.resample('1D').last().index)
        daily['open'] = minutes['open'].resample('1D').first()
        daily['high'] = minutes['high'].resample('1D').max()
        daily['low'] = minutes['low'].resample('1D').min()
        daily['close'] = minutes['close'].resample('1D').last()
        if 'volume' in minutes.columns:
            daily['volume'] = minutes['volume'].resample('1D').sum()
        if 'amount' in minutes.columns:
            daily['amount'] = minutes['amount'].resample('1D').sum()

        daily.dropna(subset=['close'], inplace=True)
        daily.index.name = 'datetime'
        return daily

    def load_minute_ohlcv(self, result_id: str, symbol: str, date: Optional[str] = None) -> pd.DataFrame:
        """动态分钟线：优先从 data_manager 拉取。

        兼容老结果：如果历史版本落盘了 minute/<symbol>.parquet，优先用。
        """
        result_dir = self.save_dir / result_id
        old_path = result_dir / "minute" / f"{symbol}.parquet"
        if old_path.exists():
            try:
                df = pd.read_parquet(old_path)
                if date:
                    date_idx = pd.to_datetime(date)
                    df = df[df.index.normalize() == date_idx.normalize()]
                return df
            except Exception:
                pass

        start_time, end_time = self._get_time_range_for_result(result_id)
        market_data_config = self._resolve_market_data_config(result_id)

        # 如果指定 date，就尽量只拉当天，避免拉全量分钟线
        if date:
            day = pd.to_datetime(date)
            # 这里按 A 股常见交易时间粗略裁剪；若数据源包含更宽时间范围也没问题
            day_start = day.strftime('%Y-%m-%d 00:00:00')
            day_end = day.strftime('%Y-%m-%d 23:59:59')
            minutes = load_stock_minutes(
                symbol,
                day_start,
                day_end,
                fields=['open', 'high', 'low', 'close', 'volume', 'amount'],
                source=market_data_config['source'],
                adjust=market_data_config['adjust'],
            )
            if minutes.empty:
                return pd.DataFrame()
            minutes = minutes[minutes.index.normalize() == day.normalize()]
            return minutes

        minutes = load_stock_minutes(
            symbol,
            start_time,
            end_time,
            fields=['open', 'high', 'low', 'close', 'volume', 'amount'],
            source=market_data_config['source'],
            adjust=market_data_config['adjust'],
        )
        return minutes

    def load_orders(
        self,
        result_id: str,
        symbol: Optional[str] = None,
        date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """加载订单记录 - 优化：使用行过滤 + 缓存减少内存占用"""
        # 检查缓存（带过滤参数的缓存）
        cache_key = self._get_cache_key(result_id, "orders", symbol, date, start_date, end_date)
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data.copy()

        result_dir = self.save_dir / result_id
        path = result_dir / "orders.parquet"
        if not path.exists():
            return pd.DataFrame()

        try:
            # 先读取必要列用于过滤，减少内存占用
            filter_cols = ["Column", "Timestamp"]
            df = pd.read_parquet(path, columns=filter_cols)

            # 应用 symbol 过滤
            if symbol:
                df = df[df["Column"] == symbol]

            # 应用日期过滤
            if date:
                date_idx = pd.to_datetime(date)
                df = df[pd.to_datetime(df["Timestamp"]).dt.normalize() == date_idx.normalize()]

            # 应用时间区间过滤
            if start_date or end_date:
                order_dates = pd.to_datetime(df["Timestamp"]).dt.normalize()
                if start_date:
                    start_dt = pd.to_datetime(start_date)
                    df = df[order_dates >= start_dt]
                if end_date:
                    end_dt = pd.to_datetime(end_date)
                    # 重新计算 order_dates（因为 df 可能已经被过滤）
                    order_dates = pd.to_datetime(df["Timestamp"]).dt.normalize()
                    df = df[order_dates <= end_dt]

            # 如果有过滤条件，重新读取完整数据（只读取过滤后的索引）
            if symbol or date or start_date or end_date:
                if df.empty:
                    self._set_cache(cache_key, df)
                    return df
                # 读取完整数据
                df = pd.read_parquet(path)
                # 重新应用过滤
                if symbol:
                    df = df[df["Column"] == symbol]
                if date:
                    date_idx = pd.to_datetime(date)
                    df = df[pd.to_datetime(df["Timestamp"]).dt.normalize() == date_idx.normalize()]
                if start_date or end_date:
                    order_dates = pd.to_datetime(df["Timestamp"]).dt.normalize()
                    if start_date:
                        start_dt = pd.to_datetime(start_date)
                        df = df[order_dates >= start_dt]
                    if end_date:
                        end_dt = pd.to_datetime(end_date)
                        order_dates = pd.to_datetime(df["Timestamp"]).dt.normalize()
                        df = df[order_dates <= end_dt]

            self._set_cache(cache_key, df)
            return df

        except Exception as e:
            print(f"加载 orders 数据时出错: {e}")
            return pd.DataFrame()

    def load_positions(self, result_id: str, symbol: Optional[str] = None, date: Optional[str] = None) -> pd.Series:
        """加载持仓数据 - 优化：使用列裁剪 + 缓存"""
        # 检查缓存
        cache_key = self._get_cache_key(result_id, "positions", symbol, date)
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data.copy()

        result_dir = self.save_dir / result_id
        path = result_dir / "positions.parquet"
        if not path.exists():
            return pd.Series(dtype=float)

        # 优化：只读取指定 symbol 列，避免加载所有股票数据
        if symbol:
            try:
                df = pd.read_parquet(path, columns=[symbol])
                if symbol in df.columns:
                    data = df[symbol]
                else:
                    return pd.Series(dtype=float)
            except Exception:
                return pd.Series(dtype=float)
        else:
            try:
                data = pd.read_parquet(path)
            except Exception:
                return pd.Series(dtype=float)

        # 应用日期过滤
        if date:
            date_idx = pd.to_datetime(date)
            data = data[data.index.normalize() == date_idx.normalize()]

        self._set_cache(cache_key, data)
        return data

    def load_trades(
        self,
        result_id: str,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """加载交易记录 - 优化：使用行过滤 + 列裁剪 + 缓存"""
        # 检查缓存
        cache_key = self._get_cache_key(result_id, "trades", symbol, start_date, end_date)
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data.copy()

        result_dir = self.save_dir / result_id
        path = result_dir / "trades.parquet"
        if not path.exists():
            return pd.DataFrame()

        try:
            # 先读取必要列用于过滤，减少内存占用
            filter_cols = ["Column"]
            if start_date or end_date:
                filter_cols.extend(["Exit Timestamp", "Entry Timestamp"])
            df = pd.read_parquet(path, columns=filter_cols)

            # 应用 symbol 过滤
            if symbol:
                df = df[df["Column"] == symbol]

            # 应用时间区间过滤（基于 Entry Timestamp 或 Exit Timestamp）
            if start_date or end_date:
                df = df.copy()
                # 优先使用 Exit Timestamp，如果不存在则使用 Entry Timestamp
                time_col = "Exit Timestamp" if "Exit Timestamp" in df.columns else "Entry Timestamp"
                if time_col in df.columns:
                    trade_dates = pd.to_datetime(df[time_col]).dt.normalize()
                    if start_date:
                        start_dt = pd.to_datetime(start_date)
                        df = df[trade_dates >= start_dt]
                    if end_date:
                        end_dt = pd.to_datetime(end_date)
                        # 重新计算 trade_dates（因为 df 可能已经被过滤）
                        trade_dates = pd.to_datetime(df[time_col]).dt.normalize()
                        df = df[trade_dates <= end_dt]

            # 如果有过滤条件，重新读取完整数据（只读取过滤后的行）
            if symbol or start_date or end_date:
                if df.empty:
                    self._set_cache(cache_key, df)
                    return df
                # 使用索引读取完整数据
                df = pd.read_parquet(path)
                if symbol:
                    df = df[df["Column"] == symbol]
                if start_date or end_date:
                    time_col = "Exit Timestamp" if "Exit Timestamp" in df.columns else "Entry Timestamp"
                    if time_col in df.columns:
                        trade_dates = pd.to_datetime(df[time_col]).dt.normalize()
                        if start_date:
                            start_dt = pd.to_datetime(start_date)
                            df = df[trade_dates >= start_dt]
                        if end_date:
                            end_dt = pd.to_datetime(end_date)
                            trade_dates = pd.to_datetime(df[time_col]).dt.normalize()
                            df = df[trade_dates <= end_dt]

            self._set_cache(cache_key, df)
            return df

        except Exception as e:
            print(f"加载 trades 数据时出错: {e}")
            return pd.DataFrame()

    def list_results(self) -> pd.DataFrame:
        results = []

        for result_dir in self.save_dir.iterdir():
            if result_dir.is_dir():
                metadata_path = result_dir / "metadata.json"
                if metadata_path.exists():
                    with open(metadata_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)

                    stats_path = result_dir / "stats.json"
                    if stats_path.exists():
                        with open(stats_path, 'r', encoding='utf-8') as f:
                            stats = json.load(f)
                        # 这里仍取全局 stats 用于列表展示
                        metadata['end_value'] = stats.get('stats', {}).get('End Value', 'N/A')
                        metadata['total_return'] = stats.get('stats', {}).get('Total Return [%]', 'N/A')

                    results.append(metadata)

        if results:
            return pd.DataFrame(results)
        return pd.DataFrame()
