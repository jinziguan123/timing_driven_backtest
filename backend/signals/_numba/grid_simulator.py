"""
网格交易模拟器 - Numba 加速版本
"""

import numpy as np
from numba import njit


@njit
def numba_grid_simulator(open_arr, close, high, low, dates,
                         total_cash, base_position_cash, fees,
                         grid_up, grid_down, trade_qty):
    """
    Numba 核心：跨网格（逐格）+ gap（open 成交）+ T+1 + 资金约束。

    - 当 open_ 相对 last_grid_price 已经跨越网格阈值：视为高开/低开，按 open_ 成交。
    - 否则：按 close 成交。

    注意：同一 bar 内可能触发多格，但输出只记录净成交量；成交价矩阵用于让引擎按对应价格撮合。
    
    Args:
        open_arr: 开盘价数组
        close: 收盘价数组
        high: 最高价数组
        low: 最低价数组
        dates: 日期数组 (YYYYMMDD 格式)
        total_cash: 总资金
        base_position_cash: 底仓资金
        fees: 手续费率
        grid_up: 上涨卖出幅度
        grid_down: 下跌买入幅度
        trade_qty: 每次交易数量
        
    Returns:
        order_sizes: 下单数量矩阵
        exec_prices: 成交价矩阵
    """
    rows, cols = close.shape

    order_sizes = np.zeros((rows, cols), dtype=np.float64)
    exec_prices = close.astype(np.float32).copy()  # 默认使用 close

    pos_total = np.zeros(cols, dtype=np.float64)   # 总持仓
    pos_frozen = np.zeros(cols, dtype=np.float64)  # 今日买入(冻结)
    last_grid_price = np.zeros(cols, dtype=np.float64)  # 网格基准（按网格级别推进）
    
    # 给每个股票都准备资金池
    cash_pool = np.full(cols, total_cash, dtype=np.float64)

    current_date = dates[0]

    for r in range(rows):
        # T+1：跨日解冻
        if dates[r] != current_date:
            current_date = dates[r]
            pos_frozen[:] = 0.0

        for c in range(cols):
            px_close = close[r, c]
            if np.isnan(px_close) or px_close <= 0:
                continue

            px_open = open_arr[r, c]
            if np.isnan(px_open) or px_open <= 0:
                px_open = px_close

            # 初始化底仓：用当前 bar close 作为成交价/基准（保持与历史行为一致）
            if last_grid_price[c] == 0:
                pos_total[c] = int(base_position_cash / px_close / 100) * 100
                last_grid_price[c] = px_close
                init_cost = px_close * pos_total[c] * (1 + fees)
                cash_pool[c] -= init_cost
                order_sizes[r, c] = pos_total[c]
                pos_frozen[c] += pos_total[c]
                exec_prices[r, c] = px_close
                continue

            ref = last_grid_price[c]

            # gap 判定：open 已跨越阈值则用 open 成交，否则用 close
            use_open = False
            if px_open >= ref * (1 + grid_up) or px_open <= ref * (1 - grid_down):
                use_open = True

            px_exec = px_open if use_open else px_close

            net_qty = 0.0

            # 逐网格卖出
            if px_exec >= ref * (1 + grid_up):
                while px_exec >= ref * (1 + grid_up):
                    sellable = pos_total[c] - pos_frozen[c]
                    if sellable < trade_qty:
                        break

                    net_qty -= trade_qty
                    pos_total[c] -= trade_qty
                    cash_pool[c] += px_exec * trade_qty * (1 - fees)

                    # 网格基准推进（按网格价推进，而非成交价）
                    ref = ref * (1 + grid_up)

            # 逐网格买入
            elif px_exec <= ref * (1 - grid_down):
                while px_exec <= ref * (1 - grid_down):
                    cost = px_exec * trade_qty * (1 + fees)
                    if cash_pool[c] < cost:
                        break

                    net_qty += trade_qty
                    pos_total[c] += trade_qty
                    pos_frozen[c] += trade_qty
                    cash_pool[c] -= cost

                    ref = ref * (1 - grid_down)

            if net_qty != 0:
                order_sizes[r, c] = net_qty
                exec_prices[r, c] = px_exec
                last_grid_price[c] = ref

    return order_sizes, exec_prices

