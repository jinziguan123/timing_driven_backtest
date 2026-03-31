"""
卖出信号生成函数 - Numba 加速版本

包含各类止盈止损逻辑的高性能实现。
"""

import numpy as np
from numba import njit


@njit
def numba_generate_bias_macd_jinzuan_sell_signal(entry_signal, close_arr):
    """
    生成卖出信号：止盈 +30% 或 止损 -10%
    
    修复：只有在未持仓时才响应买入信号，避免持仓期间成本价被覆盖
    
    Args:
        entry_signal: 买入信号矩阵 (bool)
        close_arr: 收盘价矩阵 (float64)
        
    Returns:
        sell_signal: 卖出信号矩阵 (bool)
    """
    rows, cols = close_arr.shape
    sell_signal = np.zeros((rows, cols), dtype=np.bool_)
    # 设置持仓成本
    pos_cost = np.zeros(cols, dtype=np.float64)
    
    for r in range(rows):
        for c in range(cols):
            # 检查是否能够卖出（必须先有持仓）
            if pos_cost[c] > 0:
                if close_arr[r, c] >= pos_cost[c] * 1.3:
                    # 止盈：涨幅达到 30%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                elif close_arr[r, c] <= pos_cost[c] * 0.9:
                    # 止损：跌幅达到 10%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                else:
                    sell_signal[r, c] = False
            else:
                sell_signal[r, c] = False
            
            # 检查是否能够买入：【关键修复】只有未持仓时才响应买入信号
            if entry_signal[r, c] and pos_cost[c] == 0:
                pos_cost[c] = close_arr[r, c]
    return sell_signal


@njit
def numba_generate_bias_expma_sell_signal(entry_signal, close_arr):
    """
    生成卖出信号：止盈30%，止损10%
    
    Args:
        entry_signal: 买入信号矩阵 (bool)
        close_arr: 收盘价矩阵 (float64)
        
    Returns:
        sell_signal: 卖出信号矩阵 (bool)
    """
    rows, cols = close_arr.shape
    sell_signal = np.zeros((rows, cols), dtype=np.bool_)
    # 设置持仓成本
    pos_cost = np.zeros(cols, dtype=np.float64)
    
    for r in range(rows):
        for c in range(cols):
            # 检查是否能够卖出（必须先有持仓）
            if pos_cost[c] > 0:
                if close_arr[r, c] >= pos_cost[c] * 1.3:
                    # 止盈：涨幅达到 30%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                elif close_arr[r, c] <= pos_cost[c] * 0.9:
                    # 止损：跌幅达到 10%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                else:
                    sell_signal[r, c] = False
            else:
                sell_signal[r, c] = False
            
            # 检查是否能够买入：【关键修复】只有未持仓时才响应买入信号
            if entry_signal[r, c] and pos_cost[c] == 0:
                pos_cost[c] = close_arr[r, c]
    return sell_signal


@njit
def numba_generate_week_ema_sell_signal(entry_signal, close_arr):
    """
    生成卖出信号：止盈30%，止损10%
    
    Args:
        entry_signal: 买入信号矩阵 (bool)
        close_arr: 收盘价矩阵 (float64)
        
    Returns:
        sell_signal: 卖出信号矩阵 (bool)
    """
    rows, cols = close_arr.shape
    sell_signal = np.zeros((rows, cols), dtype=np.bool_)
    # 设置持仓成本
    pos_cost = np.zeros(cols, dtype=np.float64)
    
    for r in range(rows):
        for c in range(cols):
            # 检查是否能够卖出（必须先有持仓）
            if pos_cost[c] > 0:
                if close_arr[r, c] >= pos_cost[c] * 1.3:
                    # 止盈：涨幅达到 30%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                elif close_arr[r, c] <= pos_cost[c] * 0.9:
                    # 止损：跌幅达到 10%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                else:
                    sell_signal[r, c] = False
            else:
                sell_signal[r, c] = False
            
            # 检查是否能够买入：【关键修复】只有未持仓时才响应买入信号
            if entry_signal[r, c] and pos_cost[c] == 0:
                pos_cost[c] = close_arr[r, c]
    return sell_signal

@njit
def numba_generate_fibonacci_ema_sell_signal(entry_signal, close_arr):
    """
    生成卖出信号：止盈30%，止损10%
    
    Args:
        entry_signal: 买入信号矩阵 (bool)
        close_arr: 收盘价矩阵 (float64)
        
    Returns:
        sell_signal: 卖出信号矩阵 (bool)
    """
    rows, cols = close_arr.shape
    sell_signal = np.zeros((rows, cols), dtype=np.bool_)
    # 设置持仓成本
    pos_cost = np.zeros(cols, dtype=np.float64)
    
    for r in range(rows):
        for c in range(cols):
            # 检查是否能够卖出（必须先有持仓）
            if pos_cost[c] > 0:
                if close_arr[r, c] >= pos_cost[c] * 1.2:
                    # 止盈：涨幅达到 30%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                elif close_arr[r, c] <= pos_cost[c] * 0.95:
                    # 止损：跌幅达到 10%
                    sell_signal[r, c] = True
                    pos_cost[c] = 0
                else:
                    sell_signal[r, c] = False
            else:
                sell_signal[r, c] = False
            
            # 检查是否能够买入：【关键修复】只有未持仓时才响应买入信号
            if entry_signal[r, c] and pos_cost[c] == 0:
                pos_cost[c] = close_arr[r, c]
    return sell_signal


@njit
def numba_generate_fibonacci_ema_multi_position_signals(
    entry_signal, close_arr, day_indices,
    buy_size=100, 
    take_profit=0.2, 
    stop_loss=0.05, 
    max_positions=20
):
    """
    多笔持仓跟踪的买卖信号生成器
    
    每次买入信号触发时买入固定数量股票，每笔持仓独立跟踪止盈止损。
    当某笔持仓触发止盈/止损时，卖出对应数量的股票。
    每只股票每天只能买入一次。
    
    Args:
        entry_signal: 买入信号矩阵 (bool), shape=(rows, cols)
        close_arr: 收盘价矩阵 (float64), shape=(rows, cols)
        day_indices: 每行对应的日期索引 (rows,)，用于识别新的一天
        buy_size: 每次买入数量 (默认100股)
        take_profit: 止盈比例 (默认20%)
        stop_loss: 止损比例 (默认5%)
        max_positions: 每只股票最大持仓笔数 (默认20笔)
        
    Returns:
        size_matrix: 订单数量矩阵 (正数买入，负数卖出)
    """
    rows, cols = close_arr.shape
    size_matrix = np.zeros((rows, cols), dtype=np.float32)
    
    # 对于每只股票，跟踪多笔持仓
    # positions_cost[c, p] 存储第c只股票第p笔持仓的成本价 (0表示空位)
    positions_cost = np.zeros((cols, max_positions), dtype=np.float32)
    # positions_count[c] 存储第c只股票当前的持仓笔数
    positions_count = np.zeros(cols, dtype=np.int32)
    
    # 跟踪每只股票当天是否已买入
    day_bought = np.zeros(cols, dtype=np.bool_)
    # 记录上一个日期索引
    prev_day_idx = -1
    
    for r in range(rows):
        current_day_idx = day_indices[r]
        
        # 检测新的一天，重置日内买入状态
        if current_day_idx != prev_day_idx:
            day_bought[:] = False
            prev_day_idx = current_day_idx
        
        for c in range(cols):
            current_price = close_arr[r, c]
            
            # 跳过无效数据
            if np.isnan(current_price) or current_price <= 0:
                continue
            
            # 标记本时刻是否发生了卖出
            has_sold = False
            
            # 先检查每笔持仓是否触发止盈止损
            for p in range(max_positions):
                cost = positions_cost[c, p]
                if cost > 0:
                    # 止盈
                    if current_price >= cost * (1.0 + take_profit):
                        size_matrix[r, c] -= buy_size  # 卖出
                        positions_cost[c, p] = 0
                        positions_count[c] -= 1
                        has_sold = True
                    # 止损
                    elif current_price <= cost * (1.0 - stop_loss):
                        size_matrix[r, c] -= buy_size  # 卖出
                        positions_cost[c, p] = 0
                        positions_count[c] -= 1
                        has_sold = True
            
            # 然后处理买入信号（只有还有空位且当天未买入且本时刻未卖出时才买入）
            if entry_signal[r, c] and positions_count[c] < max_positions and not day_bought[c] and not has_sold:
                # 找一个空位存放新的持仓
                for p in range(max_positions):
                    if positions_cost[c, p] == 0:
                        positions_cost[c, p] = current_price
                        positions_count[c] += 1
                        size_matrix[r, c] += buy_size  # 买入
                        day_bought[c] = True  # 标记当天已买入
                        break
    
    return size_matrix


@njit
def numba_generate_fibonacci_ema_signal_anchor_order_matrices(
    entry_signal,
    entry_price_arr,
    close_arr,
    anchor_arr,
    signal_id_arr,
    day_indices,
    buy_size=100,
    take_profit=0.1,
    stop_loss=0.1,
    max_positions=1,
):
    """
    V11 专用订单矩阵内核。

    规则：
    - 买入基于信号日收盘价锚定止盈止损；
    - 同一个日线信号窗口只允许成交一次，退出后不在同一窗口重复买回；
    - A 股 T+1：买入当天的持仓不能参与卖出判断；
    - 卖出在触发分钟确认后，下一分钟按触发分钟收盘价下单。
    """
    rows, cols = close_arr.shape
    size_matrix = np.zeros((rows, cols), dtype=np.float32)
    price_matrix = np.full((rows, cols), np.nan, dtype=np.float32)

    positions_anchor = np.zeros((cols, max_positions), dtype=np.float32)
    positions_buy_day_idx = np.full((cols, max_positions), -1, dtype=np.int32)
    positions_count = np.zeros(cols, dtype=np.int32)
    last_consumed_signal_id = np.full(cols, -1, dtype=np.int32)

    pending_exit_mask = np.zeros((cols, max_positions), dtype=np.bool_)
    pending_exit_price = np.full(cols, np.nan, dtype=np.float32)

    day_bought = np.zeros(cols, dtype=np.bool_)
    prev_day_idx = -1

    for r in range(rows):
        current_day_idx = day_indices[r]
        if current_day_idx != prev_day_idx:
            day_bought[:] = False
            prev_day_idx = current_day_idx

        for c in range(cols):
            current_price = close_arr[r, c]
            if np.isnan(current_price) or current_price <= 0:
                continue

            has_sold = False

            pending_exit_count = 0
            if not np.isnan(pending_exit_price[c]):
                for p in range(max_positions):
                    if pending_exit_mask[c, p]:
                        pending_exit_count += 1
                        pending_exit_mask[c, p] = False
                        positions_anchor[c, p] = 0.0
                        positions_buy_day_idx[c, p] = -1
                        positions_count[c] -= 1
                if pending_exit_count > 0:
                    size_matrix[r, c] -= buy_size * pending_exit_count
                    price_matrix[r, c] = pending_exit_price[c]
                    pending_exit_price[c] = np.nan
                    has_sold = True

            if entry_signal[r, c] and positions_count[c] < max_positions and not day_bought[c] and not has_sold:
                signal_id = signal_id_arr[r, c]
                if signal_id > last_consumed_signal_id[c]:
                    anchor_price = anchor_arr[r, c]
                    order_price = entry_price_arr[r, c]

                    if np.isnan(anchor_price) or anchor_price <= 0:
                        anchor_price = current_price
                    if np.isnan(order_price) or order_price <= 0:
                        order_price = current_price

                    for p in range(max_positions):
                        if positions_anchor[c, p] == 0:
                            positions_anchor[c, p] = anchor_price
                            positions_buy_day_idx[c, p] = current_day_idx
                            positions_count[c] += 1
                            size_matrix[r, c] += buy_size
                            price_matrix[r, c] = order_price
                            day_bought[c] = True
                            last_consumed_signal_id[c] = signal_id
                            break

            if positions_count[c] > 0 and r < rows - 1:
                trigger_exit = False
                for p in range(max_positions):
                    anchor_price = positions_anchor[c, p]
                    if anchor_price > 0 and positions_buy_day_idx[c, p] < current_day_idx:
                        if current_price >= anchor_price * (1.0 + take_profit):
                            trigger_exit = True
                            pending_exit_mask[c, p] = True
                        elif current_price <= anchor_price * (1.0 - stop_loss):
                            trigger_exit = True
                            pending_exit_mask[c, p] = True

                if trigger_exit:
                    pending_exit_price[c] = current_price

    return size_matrix, price_matrix


@njit
def numba_r_breaker_signals(
    close_arr, high_arr, low_arr,
    break_buy_price, observe_sell_price, reversal_sell_price,
    reversal_buy_price, observe_buy_price, break_sell_price,
    day_indices
):
    """
    R-Breaker 策略信号生成器
    
    买入逻辑：
    - 突破买入：当日价格突破"突破买入价"
    - 反转买入：当日最低价曾低于"观察买入价"，且价格向上突破"反转买入价"
    
    卖出逻辑：
    - 突破卖出：当日价格跌破"突破卖出价"
    - 反转卖出：当日最高价曾超过"观察卖出价"，且价格向下跌破"反转卖出价"
    
    Args:
        close_arr: 收盘价矩阵 (rows x cols)，分钟线数据
        high_arr: 最高价矩阵 (rows x cols)，分钟线数据
        low_arr: 最低价矩阵 (rows x cols)，分钟线数据
        break_buy_price: 突破买入价矩阵 (rows x cols)，已下放到分钟线
        observe_sell_price: 观察卖出价矩阵 (rows x cols)
        reversal_sell_price: 反转卖出价矩阵 (rows x cols)
        reversal_buy_price: 反转买入价矩阵 (rows x cols)
        observe_buy_price: 观察买入价矩阵 (rows x cols)
        break_sell_price: 突破卖出价矩阵 (rows x cols)
        day_indices: 每行对应的日期索引 (rows,)，用于识别新的一天
        
    Returns:
        entry_signal: 买入信号矩阵 (bool)
        exit_signal: 卖出信号矩阵 (bool)
    """
    rows, cols = close_arr.shape
    entry_signal = np.zeros((rows, cols), dtype=np.bool_)
    exit_signal = np.zeros((rows, cols), dtype=np.bool_)
    
    # 每只股票的持仓成本
    pos_cost = np.zeros(cols, dtype=np.float64)
    
    # 每只股票当日是否已触发观察条件
    day_high_exceeded_observe_sell = np.zeros(cols, dtype=np.bool_)  # 当日最高价超过观察卖出价
    day_low_below_observe_buy = np.zeros(cols, dtype=np.bool_)        # 当日最低价低于观察买入价
    
    # 记录上一个日期索引
    prev_day_idx = -1
    
    for r in range(rows):
        current_day_idx = day_indices[r]
        
        # 检测新的一天，重置日内状态
        if current_day_idx != prev_day_idx:
            day_high_exceeded_observe_sell[:] = False
            day_low_below_observe_buy[:] = False
            prev_day_idx = current_day_idx
        
        for c in range(cols):
            current_price = close_arr[r, c]
            current_high = high_arr[r, c]
            current_low = low_arr[r, c]
            
            # 跳过无效数据
            if np.isnan(current_price) or current_price <= 0:
                continue
            
            # 更新日内观察条件
            if current_high > observe_sell_price[r, c]:
                day_high_exceeded_observe_sell[c] = True
            if current_low < observe_buy_price[r, c]:
                day_low_below_observe_buy[c] = True
            
            # ==================== 卖出信号判断 ====================
            if pos_cost[c] > 0:
                should_sell = False
                
                # 止盈止损（基本保护）
                if current_price >= pos_cost[c] * 1.2:
                    should_sell = True
                elif current_price <= pos_cost[c] * 0.95:
                    should_sell = True
                # 突破卖出：价格跌破"突破卖出价"
                elif current_price < break_sell_price[r, c]:
                    should_sell = True
                # 反转卖出：当日最高价曾超过观察卖出价，且价格跌破反转卖出价
                elif day_high_exceeded_observe_sell[c] and current_price < reversal_sell_price[r, c]:
                    should_sell = True
                
                if should_sell:
                    exit_signal[r, c] = True
                    pos_cost[c] = 0
            
            # ==================== 买入信号判断 ====================
            if pos_cost[c] == 0:
                should_buy = False
                
                # 突破买入：价格突破"突破买入价"
                if current_price > break_buy_price[r, c]:
                    should_buy = True
                # 反转买入：当日最低价曾低于观察买入价，且价格突破反转买入价
                elif day_low_below_observe_buy[c] and current_price > reversal_buy_price[r, c]:
                    should_buy = True
                
                if should_buy:
                    entry_signal[r, c] = True
                    pos_cost[c] = current_price
    
    return entry_signal, exit_signal
