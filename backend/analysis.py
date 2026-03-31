import vectorbt as vbt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 完整汉化字典 (保持不变)
STATS_CN = {
    'Start': '开始时间', 'End': '结束时间', 'Period': '回测时长', 'Start Value': '初始资金',
    'End Value': '结束资金', 'Total Return [%]': '总收益率 [%]', 'Benchmark Return [%]': '基准收益率 [%]',
    'Max Gross Exposure [%]': '最大仓位占用 [%]', 'Total Fees Paid': '总手续费', 'Max Drawdown [%]': '最大回撤 [%]',
    'Max Drawdown Duration': '最大回撤持续时间', 'Total Trades': '总交易次数', 'Total Closed Trades': '已平仓交易数',
    'Total Open Trades': '持仓中交易数', 'Open Trade PnL': '当前持仓盈亏', 'Win Rate [%]': '胜率 [%]',
    'Best Trade [%]': '最佳单笔收益 [%]', 'Worst Trade [%]': '最差单笔收益 [%]', 'Avg Winning Trade [%]': '平均盈利 [%]',
    'Avg Losing Trade [%]': '平均亏损 [%]', 'Avg Winning Trade Duration': '平均持仓时间(赢)',
    'Avg Losing Trade Duration': '平均持仓时间(亏)', 'Profit Factor': '盈亏比', 'Expectancy': '单笔期望收益',
    'Sharpe Ratio': '夏普比率', 'Calmar Ratio': '卡玛比率', 'Omega Ratio': '欧米伽比率', 'Sortino Ratio': '索提诺比率'
}

class StrategyVisualizer:
    def __init__(self, portfolio, data_dict, position_data=None):
        self.pf = portfolio
        self.data_dict = data_dict
        # position_data: 现在这里接收的是【绝对持仓量】的DataFrame
        self.pos_data = position_data 

    def show_stats(self, stock_code=None):
        """
        打印汉化统计 (保持不变)
        """
        import warnings
        if stock_code:
            try:
                single_pf = self.pf[stock_code]
                stats = single_pf.stats()
                print(f"\n--- 股票 {stock_code} 统计报告 ---")
            except Exception:
                print(f"\n[警告] 无法单独提取 {stock_code}，显示全局统计")
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=UserWarning)
                    stats = self.pf.stats()
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                stats = self.pf.stats()
            stats.name = "策略平均表现"
            print("\n--- 全局聚合统计报告 ---")
        stats.index = [STATS_CN.get(i, i) for i in stats.index]
        print(stats)

    def plot_trade_record(self, stock_code):
        """
        绘制详细交易图表 - 增强版
        1. 价格图改为日线蜡烛图 (Candlestick)
        2. 买卖点标注在具体时间点
        """
        if stock_code not in self.data_dict['close'].columns:
            print(f"报错: 找不到股票 {stock_code} 的数据")
            return

        # 1. 准备日线 K 线数据
        # 虽然回测是分钟级的，但画图用日线更清晰
        print(f"正在为 {stock_code} 生成日线K线图数据...")
        
        # 提取分钟线数据
        min_open = self.data_dict['open'][stock_code]
        min_high = self.data_dict['high'][stock_code]
        min_low = self.data_dict['low'][stock_code]
        min_close = self.data_dict['close'][stock_code]
        
        # 合成为日线
        daily_df = pd.DataFrame()
        daily_df['open'] = min_open.resample('D').first()
        daily_df['high'] = min_high.resample('D').max()
        daily_df['low'] = min_low.resample('D').min()
        daily_df['close'] = min_close.resample('D').last()
        
        # 去除周末等无交易的空行，防止K线图出现断层
        daily_df.dropna(inplace=True)

        # 2. 准备订单数据
        all_records = self.pf.orders.records_readable
        stock_orders = all_records[all_records['Column'] == stock_code]
        buy_orders = stock_orders[stock_orders['Side'] == 'Buy']
        sell_orders = stock_orders[stock_orders['Side'] == 'Sell']
        
        print(f"股票 {stock_code} 绘图统计: 买入 {len(buy_orders)} 笔, 卖出 {len(sell_orders)} 笔")
        
        # 创建子图
        fig = make_subplots(
            rows=4, cols=1, 
            shared_xaxes=True, 
            vertical_spacing=0.03,
            row_heights=[0.5, 0.15, 0.15, 0.2], # 调整了一下比例，给K线多点空间
            subplot_titles=(
                f"{stock_code} 日线走势与买卖点", 
                "持仓股数 (绝对值)", 
                "累计盈亏",
                "策略净值曲线"
            )
        )

        # --- Row 1: 日线蜡烛图 & 买卖点标注 ---
        
        # 绘制蜡烛图
        # increasing: 上涨颜色 (红), decreasing: 下跌颜色 (绿) —— 符合A股习惯
        fig.add_trace(go.Candlestick(
            x=daily_df.index,
            open=daily_df['open'],
            high=daily_df['high'],
            low=daily_df['low'],
            close=daily_df['close'],
            name='日K线',
            increasing_line_color='red',
            decreasing_line_color='green'
        ), row=1, col=1)
        
        # 买入成交点 (红色向上箭头)
        if not buy_orders.empty:
            fig.add_trace(go.Scatter(
                x=buy_orders['Timestamp'], # 保持分钟级时间戳，Plotly会自动对齐到日线轴上
                y=buy_orders['Price'],
                mode='markers', # 去掉 text，只保留箭头，避免太拥挤，hover显示详情
                name='买入',
                marker=dict(
                    symbol='arrow-up', # 使用箭头
                    size=10,
                    color='darkred',
                    line=dict(width=1, color='black')
                ),
                customdata=list(zip(buy_orders['Size'], buy_orders['Fees'])),
                hovertemplate=(
                    "<b>买入</b><br>" +
                    "时间: %{x}<br>" +
                    "价格: %{y:.2f}<br>" +
                    "数量: %{customdata[0]:.0f} 股<br>" +
                    "手续费: %{customdata[1]:.2f}<br>" +
                    "<extra></extra>"
                )
            ), row=1, col=1)
        
        # 卖出成交点 (绿色向下箭头)
        if not sell_orders.empty:
            fig.add_trace(go.Scatter(
                x=sell_orders['Timestamp'], 
                y=sell_orders['Price'],
                mode='markers',
                name='卖出',
                marker=dict(
                    symbol='arrow-down',
                    size=10,
                    color='darkgreen',
                    line=dict(width=1, color='black')
                ),
                customdata=list(zip(sell_orders['Size'], sell_orders['Fees'])),
                hovertemplate=(
                    "<b>卖出</b><br>" +
                    "时间: %{x}<br>" +
                    "价格: %{y:.2f}<br>" +
                    "数量: %{customdata[0]:.0f} 股<br>" +
                    "手续费: %{customdata[1]:.2f}<br>" +
                    "<extra></extra>"
                )
            ), row=1, col=1)

        # --- Row 2: 持仓状态 (绝对持仓) ---
        if self.pos_data is not None:
            # 这里的 self.pos_data 现在是 portfolio.assets()，即绝对持仓
            if isinstance(self.pos_data, (pd.DataFrame, pd.Series)):
                total_pos = self.pos_data[stock_code]
                fig.add_trace(go.Scatter(
                    x=total_pos.index, 
                    y=total_pos, 
                    name='持仓股数',
                    line=dict(color='blue', width=1.5), 
                    fill='tozeroy', 
                    fillcolor='rgba(0,0,255,0.1)',
                    hovertemplate="时间: %{x}<br>持仓: %{y:.0f} 股<extra></extra>"
                ), row=2, col=1)
        else:
            fig.add_trace(go.Scatter(x=daily_df.index, y=[0]*len(daily_df), name='无数据'), row=2, col=1)

        # --- Row 3: 累计盈亏 ---
        try:
            trades = self.pf.trades.records_readable
            stock_trades = trades[trades['Column'] == stock_code]
            if not stock_trades.empty:
                closed_trades = stock_trades[stock_trades['Status'] == 'Closed']
                if not closed_trades.empty:
                    closed_trades = closed_trades.sort_values('Exit Timestamp')
                    cumulative_pnl = closed_trades['PnL'].cumsum()
                    
                    # 对齐时间轴
                    exit_times = pd.to_datetime(closed_trades['Exit Timestamp'])
                    pnl_series = pd.Series(cumulative_pnl.values, index=exit_times)
                    full_pnl = pnl_series.reindex(min_close.index).ffill().fillna(0)
                    
                    fig.add_trace(go.Scatter(
                        x=full_pnl.index,
                        y=full_pnl,
                        name='累计盈亏',
                        line=dict(color='purple', width=1.5),
                        fill='tozeroy',
                        fillcolor='rgba(128,0,128,0.1)',
                        hovertemplate="时间: %{x}<br>累计盈亏: %{y:.2f}<extra></extra>"
                    ), row=3, col=1)
                else:
                    fig.add_trace(go.Scatter(x=daily_df.index, y=[0]*len(daily_df), name='无平仓'), row=3, col=1)
            else:
                fig.add_trace(go.Scatter(x=daily_df.index, y=[0]*len(daily_df), name='无交易'), row=3, col=1)
        except Exception as e:
            print(f"绘制累计盈亏出错: {e}")

        # --- Row 4: 净值曲线 ---
        try:
            equity = self.pf.value()[stock_code]
            fig.add_trace(go.Scatter(
                x=equity.index, 
                y=equity, 
                name='净值', 
                line=dict(color='darkblue', width=1.5),
                hovertemplate="时间: %{x}<br>净值: %{y:.2f}<extra></extra>"
            ), row=4, col=1)
        except Exception:
            pass

        # 更新布局
        fig.update_layout(
            title=dict(text=f"回测分析 - {stock_code}", x=0.5),
            height=1200,
            hovermode="x unified",
            xaxis_rangeslider_visible=False, # 隐藏K线图自带的滑块，因为我们有多图联动
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        # 移除周末空隙 (Plotly特性)
        # 如果需要严格去除周末空隙，需要使用 rangebreaks，这里简单展示日历时间
        # fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])]) 

        fig.show()