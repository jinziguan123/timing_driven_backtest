import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from core.base_signal import BaseSignalGenerator
from constants import TRADE_METHOD
import data_utils
from data_manager import merge_data
from signals._numba import (
    numba_generate_fibonacci_ema_sell_signal,
    numba_generate_fibonacci_ema_multi_position_signals,
    numba_generate_fibonacci_ema_signal_anchor_order_matrices,
)


class Fibonacci_EMA_SignalGenerator(BaseSignalGenerator):
    """
    Fibonacci_EMA_SignalGenerator

    周线级别的EMA信号，采用斐波那契数列
    周期从小到大多头排列，股价上穿短期均线EMA，形成买入信号

    {周线条件}
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);
    EMA55 := EMA(CLOSE, 55);

    { 多头排列 }
    WEEK_COND := EMA21 > EMA34 AND EMA34 > EMA55;

    {日线条件}
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);
    EMA55 := EMA(CLOSE, 55);

    { 多头排列 }
    DAY_COND_1 := EMA21 > EMA34 AND EMA34 > EMA55;
    DAY_COND_2 := CROSS(CLOSE, EMA21);

    { 综合条件 }
    DAY_COND := DAY_COND_1 AND DAY_COND_2;

    { 综合条件 }
    WEEK_COND AND DAY_COND;
    """

    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        ema55 = data_utils.EMA(week_data_dict['close'], 55)
        week_cond = (ema21 > ema34) & (ema34 > ema55)
        del ema21, ema34, ema55

        # 特殊处理，将前55根k线设置为False
        week_cond.iloc[:55] = False

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        ema21 = data_utils.EMA(day_data_dict['close'], 21)
        ema34 = data_utils.EMA(day_data_dict['close'], 34)
        ema55 = data_utils.EMA(day_data_dict['close'], 55)
        day_cond_1 = (ema21 > ema34) & (ema34 > ema55)
        del ema34, ema55

        # 股价上穿短期均线 EMA21
        day_cond_2 = data_utils.CROSS(day_data_dict['close'], ema21)
        del ema21

        # 日线信号
        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        # 合成买入信号
        entry_signal = week_cond & day_cond
        del week_cond, day_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 生成卖出信号
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float64)
        exit_signal_values = numba_generate_fibonacci_ema_sell_signal(entry_signal_values, close_values)
        exit_signal = pd.DataFrame(exit_signal_values, index=data_dict['close'].index,
                                   columns=data_dict['close'].columns)

        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}


class Fibonacci_EMA_BIAS_SignalGenerator(BaseSignalGenerator):
    """
    周线级别的EMA信号，采用斐波那契数列
    周期从小到大多头排列，股价上穿短期均线EMA，形成买入信号

    {周线条件}
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);
    EMA55 := EMA(CLOSE, 55);

    { 多头排列 }
    WEEK_COND := EMA21 > EMA34 AND EMA34 > EMA55;

    {日线条件}
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);
    EMA55 := EMA(CLOSE, 55);

    { 多头排列 }
    DAY_COND_1 := EMA21 > EMA34 AND EMA34 > EMA55;
    DAY_COND_2 := CROSS(CLOSE, EMA21);

    { === 基于日均价的 BIAS 条件选股 === }

    DAVERAGE := AMOUNT / (VOL * 100);

    MA20  := MA(DAVERAGE, 20);
    MA55  := MA(DAVERAGE, 55);
    MA120 := MA(DAVERAGE, 120);

    BIAS20  := (DAVERAGE - MA20) / MA20 * 100;
    BIAS55  := (DAVERAGE - MA55) / MA55 * 100;
    BIAS120 := (DAVERAGE - MA120) / MA120 * 100;

    DIFF_BIAS  := BIAS55 - BIAS20;
    DIFF_BIAS2 := BIAS120 - BIAS55;

    { 选股条件：DIFF_BIAS > DIFF_BIAS2 }
    COND_3 := DIFF_BIAS > DIFF_BIAS2;

    { 综合条件 }
    DAY_COND := DAY_COND_1 AND DAY_COND_2;

    { 综合条件 }
    WEEK_COND AND DAY_COND;
    """

    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema21 = data_utils.EMA(week_data_dict['close'], 13)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        ema55 = data_utils.EMA(week_data_dict['close'], 55)
        week_cond = (ema21 > ema34) & (ema34 > ema55)
        del ema21, ema34, ema55

        # 特殊处理，将前55根k线设置为False
        week_cond.iloc[:55] = False

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        ema21 = data_utils.EMA(day_data_dict['close'], 13)
        ema34 = data_utils.EMA(day_data_dict['close'], 34)
        ema55 = data_utils.EMA(day_data_dict['close'], 55)
        day_cond_1 = (ema21 > ema34) & (ema34 > ema55)
        del ema34, ema55

        # 股价上穿短期均线 EMA21
        day_cond_2 = data_utils.CROSS(day_data_dict['close'], ema21)
        del ema21

        # 基于日均价的 BIAS 条件选股
        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma20 = data_utils.MA(average_price, 20)
        ma55 = data_utils.MA(average_price, 55)
        ma120 = data_utils.MA(average_price, 120)
        bias20 = (average_price - ma20) / ma20 * 100
        bias55 = (average_price - ma55) / ma55 * 100
        bias120 = (average_price - ma120) / ma120 * 100
        diff_bias = bias120 - bias20
        diff_bias2 = bias120 - bias55
        day_cond_3 = diff_bias > diff_bias2
        del average_price, ma20, ma55, ma120, bias20, bias55, bias120, diff_bias, diff_bias2

        # 日线信号
        day_cond = day_cond_1 & day_cond_2 & day_cond_3
        del day_cond_1, day_cond_2, day_cond_3

        day_cond[:120] = False

        # 合成买入信号
        entry_signal = week_cond & day_cond
        del week_cond, day_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 生成卖出信号
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float64)
        exit_signal_values = numba_generate_fibonacci_ema_sell_signal(entry_signal_values, close_values)
        exit_signal = pd.DataFrame(exit_signal_values, index=data_dict['close'].index,
                                   columns=data_dict['close'].columns)

        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}


class Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_SignalGenerator(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_SignalGenerator

    多笔持仓跟踪版本：每次买入信号买入100股，每笔持仓独立跟踪止盈止损

    周线部分用以下通达信条件选股公式
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    COND1 := EMA13 > EMA21 AND EMA21 > EMA34;

    { 综合条件 }
    COND1;


    日线部分的条件选股公式用以下公式
    第一个
    { 锁主升浪买点 - 条件选股公式 }
    AAA:=(3*C+H+L+O)/6;
    VAR1:=(8*AAA+7*REF(AAA,1)+6*REF(AAA,2)+5*REF(AAA,3)+4*REF(AAA,4)+3*REF(AAA,5)+2*REF(AAA,6)+REF(AAA,7))/36;
    VAR5:=(LLV(VAR1,2)+LLV(VAR1,4)+LLV(VAR1,8))/3;
    REF(VAR5,1)=REF(VAR1,1) AND VAR5<VAR1;



    第二个
    { === 基于日均价的 BIAS 条件选股 === }

    DAVERAGE := IF(VOL > 0, AMOUNT / (VOL * 100), CLOSE);

    MA13  := MA(DAVERAGE, 13);
    MA89 := MA(DAVERAGE, 89);

    BIAS13  := (DAVERAGE - MA13) / MA13 * 100;
    BIAS89 := (DAVERAGE - MA89) / MA89 * 100;

    DIFF_BIAS  := BIAS89 - BIAS13;


    { 选股条件：DIFF_BIAS <5 }
    DIFF_BIAS < 5;
    """

    def __init__(self, params: Dict[str, Any] = None):
        # 默认参数
        default_params = {
            'buy_size': 100,  # 每次买入股数
            'take_profit': 0.2,  # 止盈比例 20%
            'stop_loss': 0.05,  # 止损比例 5%
            'max_positions': 20  # 每只股票最大持仓笔数
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation

        返回 SIZE_AND_PRICE 模式，支持每次买入100股，每笔持仓独立止盈止损
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        # 特殊处理，将前34根k线设置为False
        week_cond.iloc[:34] = False

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        aaa = (3 * day_data_dict['close'] + day_data_dict['high'] + day_data_dict['low'] + day_data_dict['open']) / 6
        var1 = (8 * aaa + 7 * aaa.shift(1) + 6 * aaa.shift(2) + 5 * aaa.shift(3) + 4 * aaa.shift(4) + 3 * aaa.shift(
            5) + 2 * aaa.shift(6) + aaa.shift(7)) / 36
        var5 = (data_utils.LLV(var1, 2) + data_utils.LLV(var1, 4) + data_utils.LLV(var1, 8)) / 3
        day_cond_1 = (var5.shift(1) == var1.shift(1)) & (var5 < var1)
        del aaa, var1, var5

        # 基于日均价的 BIAS 条件选股
        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < 5
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        # 日线信号
        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False

        # 合成买入信号
        entry_signal = week_cond & day_cond
        del week_cond, day_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)

        # 生成多笔持仓跟踪的订单数量矩阵
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        # 转换为 DataFrame
        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 价格使用收盘价
        price_df = data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator

    多笔持仓跟踪版本：每次买入信号买入100股，每笔持仓独立跟踪止盈止损
    
    { 周线部分用以下通达信条件选股公式 }
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    WEEK_COND1 := EMA13 > EMA21 AND EMA21 > EMA34;


    {抓主升浪买点}
    DIF:=EMA(C,2);
    DEA:=EMA(SLOPE(C,30)*5+C,20);
    DAY_COND1 := CROSS(DIF,DEA);

    { === 基于日均价的 BIAS 条件选股 === }

    DAVERAGE := IF(VOL > 0, AMOUNT / (VOL * 100), CLOSE);

    MA13  := MA(DAVERAGE, 13);
    MA89 := MA(DAVERAGE, 89);

    BIAS13  := (DAVERAGE - MA13) / MA13 * 100;
    BIAS89 := (DAVERAGE - MA89) / MA89 * 100;

    DIFF_BIAS := BIAS89 - BIAS13;


    { 选股条件：DIFF_BIAS <5 }
    DAY_COND2 := DIFF_BIAS < 5;
    
    FINAL_SIGNAL : WEEK_COND1 and DAY_COND1 and DAY_COND2
    """

    def __init__(self, params: Dict[str, Any] = None):
        # 默认参数
        default_params = {
            'buy_size': 100,           # 每次买入股数
            'take_profit': 0.1,        # 止盈比例 20%
            'stop_loss': 0.1,         # 止损比例 5%
            'max_positions': 1         # 每只股票最大持仓笔数（有持仓不再加仓）
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation

        返回 SIZE_AND_PRICE 模式，支持每次买入100股，每笔持仓独立止盈止损
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        # 特殊处理，将前34根k线设置为False
        week_cond.iloc[:34] = False

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        # 基于日均价的 BIAS 条件选股
        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < -7
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        # 日线信号
        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False

        # 合成买入信号
        entry_signal = week_cond & day_cond
        del week_cond, day_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值（通常日线数据只有一个值）
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 时间门控：当日出现信号后，仅允许在14:30及之后触发买入
        time_gate = (
                (entry_signal.index.hour > 14)
                | ((entry_signal.index.hour == 14) & (entry_signal.index.minute >= 30))
        )
        entry_signal.loc[~time_gate, :] = False

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)
        
        # 生成多笔持仓跟踪的订单数量矩阵
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        # 转换为 DataFrame
        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 价格使用收盘价
        price_df = data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V2(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V2
    采用更加严谨的日线和周线数据处理方式，避免未来函数的影响

    多笔持仓跟踪版本：每次买入信号买入100股，每笔持仓独立跟踪止盈止损

    周线部分用以下通达信条件选股公式
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    COND1 := EMA13 > EMA21 AND EMA21 > EMA34;

    { 综合条件 }
    COND1;


    {抓主升浪买点}
    DIF:=EMA(C,2);
    DEA:=EMA(SLOPE(C,30)*5+C,20);
    CROSS(DIF,DEA);

    { === 基于日均价的 BIAS 条件选股 === }

    DAVERAGE := IF(VOL > 0, AMOUNT / (VOL * 100), CLOSE);

    MA13  := MA(DAVERAGE, 13);
    MA89 := MA(DAVERAGE, 89);

    BIAS13  := (DAVERAGE - MA13) / MA13 * 100;
    BIAS89 := (DAVERAGE - MA89) / MA89 * 100;

    DIFF_BIAS  := BIAS89 - BIAS13;


    { 选股条件：DIFF_BIAS <5 }
    DIFF_BIAS < 5;
    """

    def __init__(self, params: Dict[str, Any] = None):
        # 默认参数
        default_params = {
            'buy_size': 100,  # 每次买入股数
            'take_profit': 0.1,  # 止盈比例 20%
            'stop_loss': 0.1,  # 止损比例 5%
            'max_positions': 20  # 每只股票最大持仓笔数
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation

        返回 SIZE_AND_PRICE 模式，支持每次买入100股，每笔持仓独立止盈止损
        """
        day_data_dict = merge_data(data_dict, '1D', strict_mode=True, start_time='09:30:00', end_time='14:30:00')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        # 特殊处理，将前34根k线设置为False
        week_cond.iloc[:34] = False

        # 特殊处理，去除未来函数
        week_cond = week_cond.shift(1).fillna(False).astype(bool)

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        # 基于日均价的 BIAS 条件选股
        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < 0
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        # 日线信号
        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False

        # 合成买入信号
        entry_signal = week_cond & day_cond
        del week_cond, day_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值（通常日线数据只有一个值）
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)

        # 生成多笔持仓跟踪的订单数量矩阵
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        # 转换为 DataFrame
        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 价格使用收盘价
        price_df = data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V3(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V3

    多笔持仓跟踪版本：每次买入信号买入100股，每笔持仓独立跟踪止盈止损
    采用日线和分钟线的数据

    日线部分用以下通达信条件选股公式
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    COND1 := EMA13 > EMA21 AND EMA21 > EMA34;

    { 综合条件 }
    COND1;


    {抓主升浪买点}
    DIF:=EMA(C,2);
    DEA:=EMA(SLOPE(C,30)*5+C,20);
    CROSS(DIF,DEA);

    { === 基于日均价的 BIAS 条件选股 === }

    DAVERAGE := IF(VOL > 0, AMOUNT / (VOL * 100), CLOSE);

    MA13  := MA(DAVERAGE, 13);
    MA89 := MA(DAVERAGE, 89);

    BIAS13  := (DAVERAGE - MA13) / MA13 * 100;
    BIAS89 := (DAVERAGE - MA89) / MA89 * 100;

    DIFF_BIAS  := BIAS89 - BIAS13;


    { 选股条件：DIFF_BIAS <5 }
    DIFF_BIAS < 5;
    """

    def __init__(self, params: Dict[str, Any] = None):
        # 默认参数
        default_params = {
            'buy_size': 100,  # 每次买入股数
            'take_profit': 0.1,  # 止盈比例 20%
            'stop_loss': 0.1,  # 止损比例 5%
            'max_positions': 20  # 每只股票最大持仓笔数
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation

        返回 SIZE_AND_PRICE 模式，支持每次买入100股，每笔持仓独立止盈止损
        """
        day_data_dict = merge_data(data_dict, '1D')

        ema13 = data_utils.EMA(day_data_dict['close'], 13)
        ema21 = data_utils.EMA(day_data_dict['close'], 21)
        ema34 = data_utils.EMA(day_data_dict['close'], 34)
        day_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        # 特殊处理，将前34根k线设置为False
        day_cond.iloc[:34] = False

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        day_cond_normalized = day_cond.copy()
        day_cond_normalized.index = pd.to_datetime(day_cond_normalized.index).normalize()
        # 去除重复索引，保留最后一个值（通常日线数据只有一个值）
        day_cond_normalized = day_cond_normalized[~day_cond_normalized.index.duplicated(keep='last')]
        day_cond = day_cond_normalized.reindex(minute_index_normalized, method='bfill')
        day_cond.index = data_dict['close'].index

        # 日线条件
        dif = data_utils.EMA(data_dict['close'], 2)
        slope = data_utils.SLOPE(data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + data_dict['close'], 20)
        minute_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        # 基于日均价的 BIAS 条件选股
        average_price = data_dict['amount'] / (data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        minute_cond_2 = diff_bias < 0
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        # 日线信号
        minute_cond = minute_cond_1 & minute_cond_2
        del minute_cond_1, minute_cond_2

        minute_cond[:89] = False

        # 合成买入信号
        entry_signal = day_cond & minute_cond
        del day_cond, minute_cond

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)

        # 生成多笔持仓跟踪的订单数量矩阵
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        # 转换为 DataFrame
        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 价格使用收盘价
        price_df = data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V4(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V4

    多笔持仓跟踪版本：每次买入信号买入100股，每笔持仓独立跟踪止盈止损

    周线部分用以下通达信条件选股公式
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    COND1 := EMA13 > EMA21 AND EMA21 > EMA34;

    最低价小于21日均线
    COND2 := LOW < EMA(LOW, 21);

    { 综合条件 }
    WEEK_COND := COND1 AND COND2;


    {抓主升浪买点}
    DIF:=EMA(C,2);
    DEA:=EMA(SLOPE(C,30)*5+C,20);
    DAY_COND := CROSS(DIF,DEA);

    { 综合条件 }
    WEEK_COND AND DAY_COND;
    """

    def __init__(self, params: Dict[str, Any] = None):
        # 默认参数
        default_params = {
            'buy_size': 100,  # 每次买入股数
            'take_profit': 0.1,  # 止盈比例 20%
            'stop_loss': 0.1,  # 止损比例 5%
            'max_positions': 20  # 每只股票最大持仓笔数
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation

        返回 SIZE_AND_PRICE 模式，支持每次买入100股，每笔持仓独立止盈止损
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)

        # 特殊处理，将前34根k线设置为False
        week_cond.iloc[:34] = False

        # 最低价小于21日均线
        # week_cond_2 = week_data_dict['close'] < ema21

        # 综合条件
        week_cond = week_cond
        del ema13, ema21, ema34, week_data_dict

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        # 日线多头排列
        ema13 = data_utils.EMA(day_data_dict['close'], 13)
        ema21 = data_utils.EMA(day_data_dict['close'], 21)
        ema34 = data_utils.EMA(day_data_dict['close'], 34)
        day_cond_2 = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        # 基于日均价的 BIAS 条件选股
        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_3 = diff_bias < 0
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        # 日线信号
        day_cond = day_cond_1 & day_cond_2 & day_cond_3
        del day_cond_1, day_cond_2, day_cond_3

        day_cond[:89] = False

        # 合成买入信号
        entry_signal = week_cond & day_cond
        del week_cond, day_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值（通常日线数据只有一个值）
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)

        # 生成多笔持仓跟踪的订单数量矩阵
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        # 转换为 DataFrame
        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 价格使用收盘价
        price_df = data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V5(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V5

    基于 V1 版本逻辑，新增以下约束：
    1) 周线信号整体延后 1 个周线周期生效；
    2) 日线信号整体延后 1 个日线周期生效；
    3) 触发信号后在“下一交易日 09:35 分钟K线”买入；
    4) 下一交易日开盘价需低于信号日收盘价的 1%；
    5) 止盈/止损统一为 10%。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,           # 每次买入股数
            'take_profit': 0.1,        # 止盈 10%
            'stop_loss': 0.1,          # 止损 10%
            'max_positions': 1         # 单票有仓不加仓
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        返回 SIZE_AND_PRICE 模式。
        """
        # 日线/周线基础数据
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        # 关键改动：信号计算使用 14:30 分钟线收盘价，而非 15:00 日线收盘价
        day_close_1430 = merge_data(
            data_dict,
            '1D',
            strict_mode=True,
            start_time='14:30:00',
            end_time='14:30:00'
        )['close']
        week_close_1430 = merge_data(
            data_dict,
            '1W',
            strict_mode=True,
            start_time='14:30:00',
            end_time='14:30:00'
        )['close']

        day_data_dict['close'] = day_close_1430.reindex(day_data_dict['close'].index)
        week_data_dict['close'] = week_close_1430.reindex(week_data_dict['close'].index)

        # ========== 周线条件（并延后一周） ==========
        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        week_cond.iloc[:34] = False
        week_cond = week_cond.shift(1).fillna(False).astype(bool)  # 周线信号延后 1 周

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # ========== 日线条件（并延后一天） ==========
        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < -7
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False
        day_cond = day_cond.shift(1).fillna(False).astype(bool)  # 日线信号延后 1 天

        # 合成日线买入信号
        entry_signal_day = week_cond & day_cond
        del week_cond, day_cond

        # 将“信号日”平移到“下一交易日”执行
        # 后续只允许在下一交易日 09:35 买入
        entry_signal_day_next = entry_signal_day.shift(1).fillna(False).astype(bool)

        # 额外条件：下一交易日开盘价 < 信号日收盘价 * 0.99
        day_open = day_data_dict['open']
        day_close = day_data_dict['close']
        gap_down_cond = day_open < (day_close.shift(1) * 0.99)

        # 合成下一交易日的日级别执行信号
        entry_exec_day = entry_signal_day_next & gap_down_cond

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_exec_day.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 仅下一交易日 09:35 分钟K允许买入
        buy_time_gate = (
            (entry_signal.index.hour == 9)
            & (entry_signal.index.minute == 35)
        )
        entry_signal.loc[~buy_time_gate, :] = False

        # 参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        # 生成数量矩阵（多笔持仓引擎，max_positions=1 表示有仓不加仓）
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 业务要求：按开盘价买入
        price_df = data_dict['open'].copy() if 'open' in data_dict else data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V6(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V6

    基于 V5 版本逻辑，新增以下约束：
    1) 周线信号整体延后 1 个周线周期生效；
    2) 日线信号整体延后 1 个日线周期生效；
    3) 触发信号后在“下一交易日 09:35 分钟K线”买入；
    4) 次日开盘需满足低开区间：前日收盘的 -3% ~ -1%；
    5) 09:35 需满足转强确认：当根收红，且收盘价高于 09:31~09:35 VWAP；
    6) 止盈/止损统一为 10%。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,           # 每次买入股数
            'take_profit': 0.1,        # 止盈 10%
            'stop_loss': 0.1,          # 止损 10%
            'max_positions': 1         # 单票有仓不加仓
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        返回 SIZE_AND_PRICE 模式。
        """
        # 日线/周线基础数据
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        # 信号计算使用 14:30 分钟线收盘价
        day_close_1430 = merge_data(
            data_dict,
            '1D',
            strict_mode=True,
            start_time='14:30:00',
            end_time='14:30:00'
        )['close']
        week_close_1430 = merge_data(
            data_dict,
            '1W',
            strict_mode=True,
            start_time='14:30:00',
            end_time='14:30:00'
        )['close']

        day_data_dict['close'] = day_close_1430.reindex(day_data_dict['close'].index)
        week_data_dict['close'] = week_close_1430.reindex(week_data_dict['close'].index)

        # ========== 周线条件（并延后一周） ==========
        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        week_cond.iloc[:34] = False
        week_cond = week_cond.shift(1).fillna(False).astype(bool)  # 周线信号延后 1 周

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # ========== 日线条件（并延后一天） ==========
        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < -7
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False
        day_cond = day_cond.shift(1).fillna(False).astype(bool)  # 日线信号延后 1 天

        # 合成日线买入信号
        entry_signal_day = week_cond & day_cond
        del week_cond, day_cond

        # 将“信号日”平移到“下一交易日”执行
        entry_signal_day_next = entry_signal_day.shift(1).fillna(False).astype(bool)

        # 低开区间过滤：-3% ~ -1%
        day_open = day_data_dict['open']
        prev_close = day_data_dict['close'].shift(1)
        gap_range_cond = (day_open < (prev_close * 0.99)) & (day_open > (prev_close * 0.97))

        # 合成下一交易日的日级别执行信号
        entry_exec_day = entry_signal_day_next & gap_range_cond

        # 下放至分钟线级别（日级信号）
        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()

        entry_signal_normalized = entry_exec_day.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = minute_index

        # ========== 09:35 转强确认 ==========
        minute_close = data_dict['close']
        minute_open = data_dict['open'] if 'open' in data_dict else data_dict['close']

        mask_0935 = (minute_index.hour == 9) & (minute_index.minute == 35)
        close_0935 = minute_close.loc[mask_0935].copy()
        open_0935 = minute_open.loc[mask_0935].copy()
        close_0935.index = close_0935.index.normalize()
        open_0935.index = open_0935.index.normalize()

        confirm_day = close_0935 > open_0935  # 当根收红

        # 若有 amount/volume，则额外要求收盘价 > 09:31~09:35 VWAP
        if 'amount' in data_dict and 'volume' in data_dict:
            minute_amount = data_dict['amount']
            minute_volume = data_dict['volume']
            mask_0931_0935 = (
                (minute_index.hour == 9)
                & (minute_index.minute >= 31)
                & (minute_index.minute <= 35)
            )

            amount_0931_0935 = minute_amount.loc[mask_0931_0935]
            volume_0931_0935 = minute_volume.loc[mask_0931_0935]

            vwap_0935 = (
                amount_0931_0935.groupby(amount_0931_0935.index.normalize()).sum()
                / (volume_0931_0935.groupby(volume_0931_0935.index.normalize()).sum() * 100)
            )
            confirm_day = confirm_day & (close_0935 > vwap_0935.reindex(close_0935.index))

        confirm_day = confirm_day.fillna(False).astype(bool)

        confirm_normalized = confirm_day.copy()
        confirm_normalized.index = pd.to_datetime(confirm_normalized.index).normalize()
        confirm_normalized = confirm_normalized[~confirm_normalized.index.duplicated(keep='last')]
        confirm_signal = confirm_normalized.reindex(minute_index_normalized)
        confirm_signal.index = minute_index
        confirm_signal = confirm_signal.fillna(False).astype(bool)

        # 合并确认条件
        entry_signal = entry_signal & confirm_signal

        # 仅下一交易日 09:35 分钟K允许买入
        buy_time_gate = (
            (entry_signal.index.hour == 9)
            & (entry_signal.index.minute == 35)
        )
        entry_signal.loc[~buy_time_gate, :] = False

        # 参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        # 生成数量矩阵
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 按开盘价买入
        price_df = data_dict['open'].copy() if 'open' in data_dict else data_dict['close'].copy()

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V7(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V7

    基于日线信号在次日做分钟级触发买入：
    1) 周线信号整体延后 1 个周线周期生效；
    2) 计算出 a 日信号后，仅在 a+1 日执行买入判断；
    3) a+1 日逐分钟遍历：当分钟 close < a 日收盘价 * 0.98 时触发；
    4) 为避免未来函数：t 时刻触发，t+1 时刻下单；
    5) 买入价格采用触发时刻（t）的 close；
    6) 单票同日最多买入一次（底层 numba day_bought 控制）；
    7) 止盈/止损统一为 10%。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        # 日线/周线基础数据
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        # ========== 周线条件（并延后一周） ==========
        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        week_cond.iloc[:34] = False
        week_cond = week_cond.shift(1).fillna(False).astype(bool)

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # ========== 日线条件 ==========
        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del dif, dea, slope

        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < -7
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False

        # 合成 a 日日线信号
        entry_signal_day = week_cond & day_cond
        del week_cond, day_cond

        # 信号在 a 日成立，仅允许在 a+1 日执行
        entry_exec_day = entry_signal_day.shift(1).fillna(False).astype(bool)

        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()

        # 将“可执行日”下放到分钟
        entry_day_normalized = entry_exec_day.copy()
        entry_day_normalized.index = pd.to_datetime(entry_day_normalized.index).normalize()
        entry_day_normalized = entry_day_normalized[~entry_day_normalized.index.duplicated(keep='last')]
        entry_day_mask = entry_day_normalized.reindex(minute_index_normalized, method='bfill')
        entry_day_mask.index = minute_index
        entry_day_mask = entry_day_mask.fillna(False).astype(bool)

        # a+1 日分钟阈值：分钟 close < a 日收盘价 * 0.98
        prev_day_close = day_data_dict['close'].shift(1)
        trigger_threshold_day = prev_day_close * 1

        trigger_threshold_normalized = trigger_threshold_day.copy()
        trigger_threshold_normalized.index = pd.to_datetime(trigger_threshold_normalized.index).normalize()
        trigger_threshold_normalized = trigger_threshold_normalized[
            ~trigger_threshold_normalized.index.duplicated(keep='last')
        ]
        trigger_threshold = trigger_threshold_normalized.reindex(minute_index_normalized, method='bfill')
        trigger_threshold.index = minute_index

        # t 时刻触发条件
        raw_entry_signal = entry_day_mask & (data_dict['close'] < trigger_threshold)
        raw_entry_signal = raw_entry_signal.fillna(False).astype(bool)

        # 避免未来函数：t 触发，t+1 下单
        entry_signal = raw_entry_signal.shift(1).fillna(False).astype(bool)

        # 限制下单仍在同一执行日内，防止收盘最后一根触发后串到下一交易日
        entry_signal = entry_signal & entry_day_mask

        # 参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        # 生成数量矩阵（底层 day_bought 保证同票同日仅买一次）
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 买入价格采用触发时刻（t）的 close；订单在 t+1 执行，因此价格整体后移一根
        price_df = data_dict['close'].shift(1)
        if 'open' in data_dict:
            # 对首根无法回看时用 open 兜底，避免 NaN 影响回测引擎
            price_df = price_df.fillna(data_dict['open'])
        else:
            price_df = price_df.fillna(data_dict['close'])

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V8(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V8

    基于 V7 版本逻辑，参数调整如下：
    1) 止盈 18%，止损 5.5%；
    2) BIAS 阈值调整为 -4.5；
    3) 周线 EMA 组合调整为 12-22-36；
    4) 日线 MACD 调整为 12-26-9。

    其余执行逻辑与 V7 保持一致：
    - a 日信号，仅在 a+1 日分钟级执行；
    - 分钟触发条件在 t 时刻成立，t+1 时刻下单；
    - 单票同日最多买入一次；
    - 买入价格采用触发时刻（t）的 close（整体后移一根对齐下单时刻）。
    """

    def __init__(self, params: Dict[str, Any] = None):

        default_params = {
            'buy_size': 100,
            'take_profit': 0.18,
            'stop_loss': 0.055,
            'max_positions': 1
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        # 日线/周线基础数据
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        # ========== 周线条件（EMA 12-22-36，并延后一周） ==========
        ema12 = data_utils.EMA(week_data_dict['close'], 12)
        ema22 = data_utils.EMA(week_data_dict['close'], 22)
        ema36 = data_utils.EMA(week_data_dict['close'], 36)
        week_cond = (ema12 > ema22) & (ema22 > ema36)
        del ema12, ema22, ema36

        week_cond.iloc[:36] = False
        week_cond = week_cond.shift(1).fillna(False).astype(bool)

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # ========== 日线条件（MACD 12-26-9） ==========
        ema_fast = data_utils.EMA(day_data_dict['close'], 12)
        ema_slow = data_utils.EMA(day_data_dict['close'], 26)
        dif = ema_fast - ema_slow
        dea = data_utils.EMA(dif, 9)
        day_cond_1 = data_utils.CROSS(dif, dea)
        del ema_fast, ema_slow, dif, dea

        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < -4.5
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        day_cond = day_cond_1 & day_cond_2
        del day_cond_1, day_cond_2

        day_cond[:89] = False

        # 合成 a 日日线信号
        entry_signal_day = week_cond & day_cond
        del week_cond, day_cond

        # 信号在 a 日成立，仅允许在 a+1 日执行
        entry_exec_day = entry_signal_day.shift(1).fillna(False).astype(bool)

        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()

        # 将“可执行日”下放到分钟
        entry_day_normalized = entry_exec_day.copy()
        entry_day_normalized.index = pd.to_datetime(entry_day_normalized.index).normalize()
        entry_day_normalized = entry_day_normalized[~entry_day_normalized.index.duplicated(keep='last')]
        entry_day_mask = entry_day_normalized.reindex(minute_index_normalized, method='bfill')
        entry_day_mask.index = minute_index
        entry_day_mask = entry_day_mask.fillna(False).astype(bool)

        # a+1 日分钟阈值：分钟 close < a 日收盘价（与 V7 保持一致）
        prev_day_close = day_data_dict['close'].shift(1)
        trigger_threshold_day = prev_day_close * 1

        trigger_threshold_normalized = trigger_threshold_day.copy()
        trigger_threshold_normalized.index = pd.to_datetime(trigger_threshold_normalized.index).normalize()
        trigger_threshold_normalized = trigger_threshold_normalized[
            ~trigger_threshold_normalized.index.duplicated(keep='last')
        ]
        trigger_threshold = trigger_threshold_normalized.reindex(minute_index_normalized, method='bfill')
        trigger_threshold.index = minute_index

        # t 时刻触发条件
        raw_entry_signal = entry_day_mask & (data_dict['close'] < trigger_threshold)
        raw_entry_signal = raw_entry_signal.fillna(False).astype(bool)

        # 避免未来函数：t 触发，t+1 下单
        entry_signal = raw_entry_signal.shift(1).fillna(False).astype(bool)

        # 限制下单仍在同一执行日内，防止收盘最后一根触发后串到下一交易日
        entry_signal = entry_signal & entry_day_mask

        # 参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.18)
        stop_loss = self.params.get('stop_loss', 0.055)
        max_positions = self.params.get('max_positions', 1)

        # 生成数量矩阵（底层 day_bought 保证同票同日仅买一次）
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 买入价格采用触发时刻（t）的 close；订单在 t+1 执行，因此价格整体后移一根
        price_df = data_dict['close'].shift(1)
        if 'open' in data_dict:
            # 对首根无法回看时用 open 兜底，避免 NaN 影响回测引擎
            price_df = price_df.fillna(data_dict['open'])
        else:
            price_df = price_df.fillna(data_dict['close'])

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V9(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V9

    基于 V7 版本逻辑，规则调整如下：
    1) 周线条件保持不变：EMA13 > EMA21 > EMA34，并整体延后一周生效；
    2) 日线信号改为：EMA(8) 的 SLOPE 大于 20；
    3) 买点仍为信号后一天（a+1）执行；
    4) 分钟触发阈值改为：分钟 close < (执行日 T 的前天收盘价 * 0.98)；
    5) t 时刻触发，t+1 时刻下单，且限制在同一执行日内；
    6) 止盈止损均为 10%。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1,
            'slope_period': 2,
            'slope_threshold': 10,
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        # 日线/周线基础数据
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        # ========== 周线条件（EMA 13-21-34，并延后一周） ==========
        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond_1 = (ema13 > ema21) & (ema21 > ema34)

        week_cond_1.iloc[:34] = False
        
        # 计算斜率
        slope13 = ((ema13 - data_utils.REF(ema13, 3)) / 3) / week_data_dict['close'] * 100
        week_cond_2 = slope13 <= 0.15
        week_cond_2.iloc[:13] = False
        
        # 股价空头排列
        week_bearish_alignment = (week_data_dict['close'] <= data_utils.REF(week_data_dict['close'], 1)) & (data_utils.REF(week_data_dict['close'], 1) <= data_utils.REF(week_data_dict['close'], 2)) & (data_utils.REF(week_data_dict['close'], 2) <= data_utils.REF(week_data_dict['close'], 3))
        
        week_bearish_alignment.iloc[:3] = False
        
        week_pull_back = week_data_dict['close'] <= data_utils.REF(week_data_dict['close'], 3) * 0.85
        week_pull_back.iloc[:3] = False
        
        week_cond3 = week_bearish_alignment & week_pull_back
        week_cond = week_cond_1 & week_cond_2 & week_cond3
        del ema13, ema21, ema34, week_cond_1, week_cond_2, slope13, week_bearish_alignment, week_pull_back, week_cond3
        
        # 延后一周
        week_cond = week_cond.shift(1).fillna(True).astype(bool)
        
        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')
        
        # 计算日线信号
        """
        跟庄波段
        RSV := (CLOSE - LLV(LOW, 9)) / (HHV(HIGH, 9) - LLV(LOW, 9)) * 100;
        K := SMA(RSV, 3, 1);
        D := SMA(K, 3, 1);
        J := 3 * K - 2 * D;

        XG: CROSS(J, 3);
        """
        rsv = (day_data_dict['close'] - data_utils.LLV(day_data_dict['low'], 9)) / (data_utils.HHV(day_data_dict['high'], 9) - data_utils.LLV(day_data_dict['low'], 9)) * 100
        k = data_utils.SMA(rsv, 3, 1)
        d = data_utils.SMA(k, 3, 1)
        j = 3 * k - 2 * d
        day_cond = data_utils.CROSS(j, 3)
        del rsv, k, d, j
        
        
        
        
        # 合成 a 日日线信号
        entry_signal_day = week_cond & day_cond
        del week_cond, day_cond

        # 信号在 a 日成立，仅允许在 a+1 日执行
        entry_exec_day = entry_signal_day.shift(1).fillna(False).astype(bool)
        del entry_signal_day

        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()

        # 将“可执行日”下放到分钟
        entry_day_normalized = entry_exec_day.copy()
        del entry_exec_day
        entry_day_normalized.index = pd.to_datetime(entry_day_normalized.index).normalize()
        entry_day_normalized = entry_day_normalized[~entry_day_normalized.index.duplicated(keep='last')]
        entry_day_mask = entry_day_normalized.reindex(minute_index_normalized, method='bfill')
        del entry_day_normalized
        entry_day_mask.index = minute_index
        entry_day_mask = entry_day_mask.fillna(False).astype(bool)

        # a+1 日分钟阈值：分钟 close < 执行日 T 的前天收盘价 * 0.98
        trigger_threshold_day = day_data_dict['close'].shift(1) * 0.98

        trigger_threshold_normalized = trigger_threshold_day.copy()
        del trigger_threshold_day
        trigger_threshold_normalized.index = pd.to_datetime(trigger_threshold_normalized.index).normalize()
        trigger_threshold_normalized = trigger_threshold_normalized[
            ~trigger_threshold_normalized.index.duplicated(keep='last')
        ]
        trigger_threshold = trigger_threshold_normalized.reindex(minute_index_normalized, method='bfill')
        del trigger_threshold_normalized
        trigger_threshold.index = minute_index

        # t 时刻触发条件
        raw_entry_signal = entry_day_mask & (data_dict['close'] < trigger_threshold)
        del trigger_threshold
        raw_entry_signal = raw_entry_signal.fillna(False).astype(bool)

        # 避免未来函数：t 触发，t+1 下单
        entry_signal = raw_entry_signal.shift(1).fillna(False).astype(bool)
        del raw_entry_signal
        # 限制下单仍在同一执行日内，防止收盘最后一根触发后串到下一交易日
        entry_signal = entry_signal & entry_day_mask
        del entry_day_mask

        # 参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        # 生成数量矩阵（底层 day_bought 保证同票同日仅买一次）
        entry_signal_values = entry_signal.values
        del entry_signal
        close_values = data_dict['close'].values.astype(np.float32)

        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=data_dict['close'].index, columns=data_dict['close'].columns)

        # 买入价格采用触发时刻（t）的 close；订单在 t+1 执行，因此价格整体后移一根
        price_df = data_dict['close'].shift(1)
        if 'open' in data_dict:
            # 对首根无法回看时用 open 兜底，避免 NaN 影响回测引擎
            price_df = price_df.fillna(data_dict['open'])
        else:
            price_df = price_df.fillna(data_dict['close'])

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V10(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V10

    基于 V9 的执行框架，买入条件改为：
    1) 周线 EMA13 > EMA21 > EMA34；
    2) 周线 KDJ 的 J < 20；
    3) 周线信号延后一周生效；
    4) 分钟级布林下轨破位：LOW < LOWER；
    5) t 时刻触发，t+1 时刻下单；
    6) 同日限制，避免最后一分钟触发串到下一交易日。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1,
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond1 = (ema13 > ema21) & (ema21 > ema34)
        week_cond1.iloc[:34] = False
        del ema13, ema21, ema34

        lowest_low = data_utils.LLV(week_data_dict['low'], 9)
        highest_high = data_utils.HHV(week_data_dict['high'], 9)
        denominator = (highest_high - lowest_low).replace(0, np.nan)
        rsv = ((week_data_dict['close'] - lowest_low) / denominator) * 100
        rsv = rsv.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        del lowest_low, highest_high, denominator
        k = data_utils.SMA(rsv, 3, 1)
        d = data_utils.SMA(k, 3, 1)
        j = 3 * k - 2 * d
        
        week_cond2 = (j < 20)
        week_cond2.iloc[:34] = False

        week_cond = week_cond1 & week_cond2
        week_cond = week_cond.shift(1, fill_value=False)

        minute_index = data_dict['close'].index
        minute_dates = minute_index.normalize()

        week_signal_normalized = week_cond.copy()
        week_signal_normalized.index = pd.to_datetime(week_signal_normalized.index).normalize()
        week_signal_normalized = week_signal_normalized[~week_signal_normalized.index.duplicated(keep='last')]
        week_mask = week_signal_normalized.reindex(minute_dates, method='bfill')
        week_mask.index = minute_index
        week_mask = week_mask.fillna(False).astype(bool)

        mid = data_utils.MA(data_dict['close'], 20)
        std = data_utils.STD(data_dict['close'], 20)
        lower = mid - 2 * std
        minute_signal = (data_dict['low'] < lower).fillna(False).astype(bool)

        raw_entry_signal = week_mask & minute_signal

        same_day_as_prev = np.zeros(len(minute_index), dtype=bool)
        if len(minute_index) > 1:
            same_day_as_prev[1:] = minute_dates[1:] == minute_dates[:-1]
        same_day_mask = pd.DataFrame(
            np.repeat(same_day_as_prev[:, None], raw_entry_signal.shape[1], axis=1),
            index=minute_index,
            columns=raw_entry_signal.columns,
        )

        entry_signal = raw_entry_signal.shift(1, fill_value=False)
        entry_signal = entry_signal & same_day_mask

        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float32)

        unique_dates = minute_dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in minute_dates], dtype=np.int16)

        size_matrix = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values,
            close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=minute_index, columns=data_dict['close'].columns)

        price_df = data_dict['close'].shift(1)
        if 'open' in data_dict:
            price_df = price_df.fillna(data_dict['open'])
        else:
            price_df = price_df.fillna(data_dict['close'])

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11

    基于 V7 的执行框架，规则调整如下：
    1) 周线条件保持不变：EMA13 > EMA21 > EMA34，并整体延后一周生效；
    2) 日线信号改为：BIAS89 < -5 且出现红箭买入信号；
    3) 红箭买入信号定义为：CROSS(EMA(C, 2), EMA(SLOPE(C, 30) * 5 + C, 20))；
    4) 日线信号出现在 a 日后，仅允许在 a+1、a+2、a+3 三个交易日内执行；
    5) 分钟触发阈值固定为：分钟 close < a 日收盘价 * 0.97；
    6) 止盈止损基于 a 日收盘价上下 10% 计算，而不是基于实际成交价；
    7) t 时刻触发，t+1 时刻下单，且不能跨交易日串单。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)

        week_cond.iloc[:34] = False
        week_cond = week_cond.shift(1).fillna(False).astype(bool)
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        ma89 = data_utils.MA(day_data_dict['close'], 89)
        bias89 = (day_data_dict['close'] - ma89) / ma89 * 100
        day_cond_1 = bias89 < -5

        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_2 = data_utils.CROSS(dif, dea)

        day_cond = day_cond_1 & day_cond_2
        day_cond[:89] = False

        entry_signal_day = week_cond & day_cond

        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()

        def expand_to_minute(day_frame: pd.DataFrame) -> pd.DataFrame:
            expanded = day_frame.copy()
            expanded.index = pd.to_datetime(expanded.index).normalize()
            expanded = expanded[~expanded.index.duplicated(keep='last')]
            expanded = expanded.reindex(minute_index_normalized, method='bfill')
            expanded.index = minute_index
            return expanded

        raw_entry_signal = pd.DataFrame(False, index=minute_index, columns=data_dict['close'].columns)
        raw_entry_anchor_price = pd.DataFrame(np.nan, index=minute_index, columns=data_dict['close'].columns)
        raw_entry_signal_id = pd.DataFrame(-1, index=minute_index, columns=data_dict['close'].columns, dtype=np.int32)
        signal_close = day_data_dict['close']
        signal_id_day = pd.DataFrame(
            -1,
            index=day_data_dict['close'].index,
            columns=day_data_dict['close'].columns,
            dtype=np.int32,
        )
        signal_id_values = np.arange(len(signal_id_day.index), dtype=np.int32)
        for column in signal_id_day.columns:
            signal_id_day[column] = np.where(entry_signal_day[column].values, signal_id_values, -1)

        for offset in range(1, 4):
            exec_day_mask = entry_signal_day.shift(offset).fillna(False).astype(bool)
            exec_day_mask = expand_to_minute(exec_day_mask).fillna(False).astype(bool)

            signal_close_day = signal_close.shift(offset)
            trigger_threshold_day = signal_close_day * 0.97
            trigger_threshold = expand_to_minute(trigger_threshold_day)
            signal_anchor_price = expand_to_minute(signal_close_day)
            signal_id_exec_day = signal_id_day.shift(offset).fillna(-1).astype(np.int32)
            signal_id_minute = expand_to_minute(signal_id_exec_day).fillna(-1).astype(np.int32)

            candidate_signal = exec_day_mask & (data_dict['close'] < trigger_threshold)
            # 多个信号窗口重叠时，优先使用最近信号日的锚定价。
            anchor_assign_mask = candidate_signal & raw_entry_anchor_price.isna()
            signal_assign_mask = candidate_signal & (raw_entry_signal_id < 0)

            raw_entry_signal = raw_entry_signal | candidate_signal
            raw_entry_anchor_price = raw_entry_anchor_price.where(~anchor_assign_mask, signal_anchor_price)
            raw_entry_signal_id = raw_entry_signal_id.where(~signal_assign_mask, signal_id_minute)

        raw_entry_signal = raw_entry_signal.fillna(False).astype(bool)
        raw_entry_anchor_price = raw_entry_anchor_price.astype(np.float32)
        raw_entry_signal_id = raw_entry_signal_id.astype(np.int32)

        same_day_as_prev = np.zeros(len(minute_index), dtype=bool)
        if len(minute_index) > 1:
            same_day_as_prev[1:] = minute_index_normalized[1:] == minute_index_normalized[:-1]
        same_day_mask = pd.DataFrame(
            np.repeat(same_day_as_prev[:, None], raw_entry_signal.shape[1], axis=1),
            index=minute_index,
            columns=raw_entry_signal.columns,
        )

        entry_signal = raw_entry_signal.shift(1, fill_value=False)
        entry_signal = entry_signal & same_day_mask
        entry_anchor_price = raw_entry_anchor_price.shift(1)
        entry_anchor_price = entry_anchor_price.where(entry_signal)
        entry_signal_id = raw_entry_signal_id.shift(1, fill_value=-1).astype(np.int32)
        entry_signal_id = entry_signal_id.where(entry_signal, -1).astype(np.int32)
        entry_order_price = data_dict['close'].shift(1)
        if 'open' in data_dict:
            entry_order_price = entry_order_price.fillna(data_dict['open'])
        else:
            entry_order_price = entry_order_price.fillna(data_dict['close'])
        entry_order_price = entry_order_price.where(entry_signal)

        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        entry_signal_values = entry_signal.values
        entry_price_values = entry_order_price.values.astype(np.float32)
        close_values = data_dict['close'].values.astype(np.float32)
        anchor_values = entry_anchor_price.values.astype(np.float32)
        signal_id_values = entry_signal_id.values.astype(np.int32)

        dates = minute_index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix, price_matrix = numba_generate_fibonacci_ema_signal_anchor_order_matrices(
            entry_signal_values,
            entry_price_values,
            close_values,
            anchor_values,
            signal_id_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=minute_index, columns=data_dict['close'].columns)
        price_df = pd.DataFrame(price_matrix, index=minute_index, columns=data_dict['close'].columns)

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V12(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V12

    完全沿用 V11 的周线、日线和卖出规则，仅调整买入方式：
    1) 周线条件保持不变：EMA13 > EMA21 > EMA34，并整体延后一周生效；
    2) 日线信号保持不变：BIAS89 < -5 且出现红箭买入信号；
    3) 红箭买入信号定义为：CROSS(EMA(C, 2), EMA(SLOPE(C, 30) * 5 + C, 20))；
    4) 买入改为：信号日当天最后一根分钟线直接买入；
    5) 买入价为该分钟线收盘价；
    6) 止盈止损仍基于信号日收盘价上下 10% 计算；
    7) 卖出继续遵守 A 股 T+1，并在触发分钟确认后下一分钟按触发分钟收盘价下单；
    8) 同一个日线信号窗口只允许成交一次，退出后不在同一窗口重复买回。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)

        week_cond.iloc[:34] = False
        week_cond = week_cond.shift(1).fillna(False).astype(bool)
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        ma89 = data_utils.MA(day_data_dict['close'], 89)
        bias89 = (day_data_dict['close'] - ma89) / ma89 * 100
        day_cond_1 = bias89 < -5

        dif = data_utils.EMA(day_data_dict['close'], 2)
        slope = data_utils.SLOPE(day_data_dict['close'], 30)
        dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
        day_cond_2 = data_utils.CROSS(dif, dea)

        day_cond = day_cond_1 & day_cond_2
        day_cond[:89] = False

        entry_signal_day = week_cond & day_cond

        minute_index = data_dict['close'].index
        minute_index_normalized = minute_index.normalize()

        def expand_to_minute(day_frame: pd.DataFrame) -> pd.DataFrame:
            expanded = day_frame.copy()
            expanded.index = pd.to_datetime(expanded.index).normalize()
            expanded = expanded[~expanded.index.duplicated(keep='last')]
            expanded = expanded.reindex(minute_index_normalized, method='bfill')
            expanded.index = minute_index
            return expanded

        last_bar_of_day = np.zeros(len(minute_index), dtype=bool)
        if len(minute_index) > 0:
            last_bar_of_day[-1] = True
        if len(minute_index) > 1:
            last_bar_of_day[:-1] = minute_index_normalized[:-1] != minute_index_normalized[1:]
        last_bar_mask = pd.DataFrame(
            np.repeat(last_bar_of_day[:, None], data_dict['close'].shape[1], axis=1),
            index=minute_index,
            columns=data_dict['close'].columns,
        )

        signal_day_mask = expand_to_minute(entry_signal_day).fillna(False).astype(bool)
        entry_signal = signal_day_mask & last_bar_mask

        signal_close = expand_to_minute(day_data_dict['close'])
        entry_anchor_price = signal_close.where(entry_signal).astype(np.float32)

        signal_id_day = pd.DataFrame(
            -1,
            index=day_data_dict['close'].index,
            columns=day_data_dict['close'].columns,
            dtype=np.int32,
        )
        signal_id_values = np.arange(len(signal_id_day.index), dtype=np.int32)
        for column in signal_id_day.columns:
            signal_id_day[column] = np.where(entry_signal_day[column].values, signal_id_values, -1)
        entry_signal_id = expand_to_minute(signal_id_day).fillna(-1).astype(np.int32)
        entry_signal_id = entry_signal_id.where(entry_signal, -1).astype(np.int32)

        entry_order_price = data_dict['close'].where(entry_signal).astype(np.float32)

        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.1)
        stop_loss = self.params.get('stop_loss', 0.1)
        max_positions = self.params.get('max_positions', 1)

        entry_signal_values = entry_signal.values
        entry_price_values = entry_order_price.values.astype(np.float32)
        close_values = data_dict['close'].values.astype(np.float32)
        anchor_values = entry_anchor_price.values.astype(np.float32)
        signal_id_values = entry_signal_id.values.astype(np.int32)

        dates = minute_index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        size_matrix, price_matrix = numba_generate_fibonacci_ema_signal_anchor_order_matrices(
            entry_signal_values,
            entry_price_values,
            close_values,
            anchor_values,
            signal_id_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )

        size_df = pd.DataFrame(size_matrix, index=minute_index, columns=data_dict['close'].columns)
        price_df = pd.DataFrame(price_matrix, index=minute_index, columns=data_dict['close'].columns)

        return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


def _run_fibonacci_ema_bias_grab_main_up_wave_signal_generator_v13_v14(
    data_dict: Dict[str, pd.DataFrame],
    params: Dict[str, Any],
    anchor_mode: str,
) -> Tuple[Any, Dict[str, Any]]:
    day_data_dict = merge_data(data_dict, '1D')
    week_data_dict = merge_data(data_dict, '1W')

    ema13 = data_utils.EMA(week_data_dict['close'], 13)
    ema21 = data_utils.EMA(week_data_dict['close'], 21)
    ema34 = data_utils.EMA(week_data_dict['close'], 34)
    week_cond = (ema13 > ema21) & (ema21 > ema34)

    week_cond.iloc[:34] = False
    week_cond = week_cond.shift(1).fillna(False).astype(bool)
    week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

    ma89 = data_utils.MA(day_data_dict['close'], 89)
    bias89 = (day_data_dict['close'] - ma89) / ma89 * 100
    day_cond_1 = bias89 < -5

    dif = data_utils.EMA(day_data_dict['close'], 2)
    slope = data_utils.SLOPE(day_data_dict['close'], 30)
    dea = data_utils.EMA(slope * 5 + day_data_dict['close'], 20)
    day_cond_2 = data_utils.CROSS(dif, dea)

    day_cond = day_cond_1 & day_cond_2
    day_cond[:89] = False

    entry_signal_day = week_cond & day_cond

    minute_index = data_dict['close'].index
    minute_index_normalized = minute_index.normalize()

    def expand_to_minute(day_frame: pd.DataFrame) -> pd.DataFrame:
        expanded = day_frame.copy()
        expanded.index = pd.to_datetime(expanded.index).normalize()
        expanded = expanded[~expanded.index.duplicated(keep='last')]
        expanded = expanded.reindex(minute_index_normalized, method='bfill')
        expanded.index = minute_index
        return expanded

    raw_entry_signal = pd.DataFrame(False, index=minute_index, columns=data_dict['close'].columns)
    raw_entry_signal_id = pd.DataFrame(-1, index=minute_index, columns=data_dict['close'].columns, dtype=np.int32)
    raw_entry_anchor_price = pd.DataFrame(np.nan, index=minute_index, columns=data_dict['close'].columns)

    signal_close = day_data_dict['close']
    signal_id_day = pd.DataFrame(
        -1,
        index=day_data_dict['close'].index,
        columns=day_data_dict['close'].columns,
        dtype=np.int32,
    )
    signal_id_values = np.arange(len(signal_id_day.index), dtype=np.int32)
    for column in signal_id_day.columns:
        signal_id_day[column] = np.where(entry_signal_day[column].values, signal_id_values, -1)

    prev_minute_close = data_dict['close'].shift(1)

    for offset in range(1, 4):
        exec_day_mask = entry_signal_day.shift(offset).fillna(False).astype(bool)
        exec_day_mask = expand_to_minute(exec_day_mask).fillna(False).astype(bool)

        signal_close_day = signal_close.shift(offset)
        trigger_threshold = expand_to_minute(signal_close_day)
        signal_id_exec_day = signal_id_day.shift(offset).fillna(-1).astype(np.int32)
        signal_id_minute = expand_to_minute(signal_id_exec_day).fillna(-1).astype(np.int32)

        candidate_signal = (
            exec_day_mask
            & (prev_minute_close >= trigger_threshold)
            & (data_dict['close'] < trigger_threshold * 0.98)
        )
        signal_assign_mask = candidate_signal & (raw_entry_signal_id < 0)

        raw_entry_signal = raw_entry_signal | candidate_signal
        raw_entry_signal_id = raw_entry_signal_id.where(~signal_assign_mask, signal_id_minute)

        if anchor_mode == 'signal_close':
            signal_anchor_price = expand_to_minute(signal_close_day)
            anchor_assign_mask = candidate_signal & raw_entry_anchor_price.isna()
            raw_entry_anchor_price = raw_entry_anchor_price.where(~anchor_assign_mask, signal_anchor_price)

    same_day_as_prev = np.zeros(len(minute_index), dtype=bool)
    if len(minute_index) > 1:
        same_day_as_prev[1:] = minute_index_normalized[1:] == minute_index_normalized[:-1]
    same_day_mask = pd.DataFrame(
        np.repeat(same_day_as_prev[:, None], raw_entry_signal.shape[1], axis=1),
        index=minute_index,
        columns=raw_entry_signal.columns,
    )

    entry_signal = raw_entry_signal.shift(1, fill_value=False)
    entry_signal = entry_signal & same_day_mask
    entry_signal_id = raw_entry_signal_id.shift(1, fill_value=-1).astype(np.int32)
    entry_signal_id = entry_signal_id.where(entry_signal, -1).astype(np.int32)

    entry_order_price = data_dict['close'].shift(1)
    if 'open' in data_dict:
        entry_order_price = entry_order_price.fillna(data_dict['open'])
    else:
        entry_order_price = entry_order_price.fillna(data_dict['close'])
    entry_order_price = entry_order_price.where(entry_signal)

    if anchor_mode == 'signal_close':
        entry_anchor_price = raw_entry_anchor_price.shift(1)
        entry_anchor_price = entry_anchor_price.where(entry_signal)
    elif anchor_mode == 'entry_price':
        entry_anchor_price = entry_order_price.where(entry_signal)
    else:
        raise ValueError(f"unsupported anchor_mode: {anchor_mode}")

    buy_size = params.get('buy_size', 100)
    take_profit = params.get('take_profit', 0.1)
    stop_loss = params.get('stop_loss', 0.1)
    max_positions = params.get('max_positions', 1)

    entry_signal_values = entry_signal.values
    entry_price_values = entry_order_price.values.astype(np.float32)
    close_values = data_dict['close'].values.astype(np.float32)
    anchor_values = entry_anchor_price.values.astype(np.float32)
    signal_id_values = entry_signal_id.values.astype(np.int32)

    dates = minute_index.normalize()
    unique_dates = dates.unique()
    date_to_idx = {d: i for i, d in enumerate(unique_dates)}
    day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

    size_matrix, price_matrix = numba_generate_fibonacci_ema_signal_anchor_order_matrices(
        entry_signal_values,
        entry_price_values,
        close_values,
        anchor_values,
        signal_id_values,
        day_indices,
        buy_size=buy_size,
        take_profit=take_profit,
        stop_loss=stop_loss,
        max_positions=max_positions
    )

    size_df = pd.DataFrame(size_matrix, index=minute_index, columns=data_dict['close'].columns)
    price_df = pd.DataFrame(price_matrix, index=minute_index, columns=data_dict['close'].columns)

    return TRADE_METHOD.SIZE_AND_PRICE, {"size": size_df, "price": price_df}


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V13(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V13

    在 V12 的基础上改为：
    1) 周线、日线条件保持与 V12 一致；
    2) 日线信号后仅允许在后 3 个交易日内执行；
    3) 分钟买点改为：价格从上向下跌破信号日收盘价；
    4) t 时刻触发，t+1 时刻按触发分钟收盘价下单；
    5) 止盈止损锚定信号日收盘价；
    6) 卖出继续遵守 A 股 T+1 与同一信号窗口不重复买回。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        return _run_fibonacci_ema_bias_grab_main_up_wave_signal_generator_v13_v14(
            data_dict,
            self.params,
            anchor_mode='signal_close',
        )


class Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V14(BaseSignalGenerator):
    """
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V14

    与 V13 的买入逻辑完全一致，差异只有：
    1) 止盈止损锚定实际买入价；
    2) 其余规则保持一致。
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,
            'take_profit': 0.1,
            'stop_loss': 0.1,
            'max_positions': 1
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[Any, Dict[str, Any]]:
        return _run_fibonacci_ema_bias_grab_main_up_wave_signal_generator_v13_v14(
            data_dict,
            self.params,
            anchor_mode='entry_price',
        )


class Fibonacci_EMA_TOP_ENTRANCE_SignalGenerator(BaseSignalGenerator):
    """
    Fibonacci_EMA_TOP_ENTRANCE_SignalGenerator

    周线条件:
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    COND1 := EMA13 > EMA21 AND EMA21 > EMA34;


    日线条件：
    { 多方趋势 }
    多方趋势:=7*SMA((CLOSE-LLV(LOW,27))/(HHV(HIGH,27)-LLV(LOW,27))*30,4,1)
          -3*SMA(SMA((CLOSE-LLV(LOW,27))/(HHV(HIGH,27)-LLV(LOW,27))*30,4,1),3,1)
          -SMA(SMA(SMA((CLOSE-LLV(LOW,27))/(HHV(HIGH,27)-LLV(LOW,27))*30,4,1),3,1),2,1);

    选股:CROSS(多方趋势,14);
    """

    def __init__(self, params: Dict[str, Any] = None):
        default_params = {
            'buy_size': 100,  # 每次买入股数
            'take_profit': 0.1,  # 止盈比例 20%
            'stop_loss': 0.1,  # 止损比例 5%
            'max_positions': 20  # 每只股票最大持仓笔数
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation

        返回 SIZE_AND_PRICE 模式，支持每次买入100股，每笔持仓独立止盈止损
        """
        day_data_dict = merge_data(data_dict, '1D')
        week_data_dict = merge_data(data_dict, '1W')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)

        # 特殊处理，将前34根k线设置为False
        week_cond.iloc[:34] = False

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        tmp = (day_data_dict['close'] - data_utils.LLV(day_data_dict['low'], 27)) / (
                    data_utils.HHV(day_data_dict['high'], 27) - data_utils.LLV(day_data_dict['low'], 27)) * 30
        a = data_utils.SMA(tmp, 4, 1)
        multi_trend = 7 * a
        b = data_utils.SMA(a, 3, 1)
        c = data_utils.SMA(b, 2, 1)
        multi_trend = 7 * a - 3 * b - c
        day_cond_1 = data_utils.CROSS(multi_trend, 14)
        del multi_trend, a, b, c, tmp

        # 乖离率
        average_price = day_data_dict['amount'] / (day_data_dict['volume'] * 100)
        ma13 = data_utils.MA(average_price, 13)
        ma89 = data_utils.MA(average_price, 89)
        bias13 = (average_price - ma13) / ma13 * 100
        bias89 = (average_price - ma89) / ma89 * 100
        diff_bias = bias89 - bias13
        day_cond_2 = diff_bias < 0
        day_cond_2[:89] = False
        del average_price, ma13, ma89, bias13, bias89, diff_bias

        # 合成买入信号
        entry_signal = week_cond & day_cond_1 & day_cond_2
        del week_cond, day_cond_1, day_cond_2

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        # 生成卖出信号
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float64)
        exit_signal_values = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values, close_values, day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )
        exit_signal = pd.DataFrame(exit_signal_values, index=data_dict['close'].index,
                                   columns=data_dict['close'].columns)

        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}


class Fibonacci_EMA_CROSS_SignalGenerator(BaseSignalGenerator):
    """
    Fibonacci_EMA_CROSS_SignalGenerator

    { EMA均线多头排列 + 金叉选股 }

    周线条件:
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);

    { 多头排列 }
    WEEK_COND1 := EMA13 > EMA21 AND EMA21 > EMA34;

    日线条件：
    EMA13 := EMA(CLOSE, 13);
    EMA21 := EMA(CLOSE, 21);
    EMA34 := EMA(CLOSE, 34);
    EMA55 := EMA(CLOSE, 55);

    DAY_COND_1 := EMA13 > EMA21 AND EMA21 > EMA55;
    DAY_COND_2 := CROSS(EMA21, EMA34);

    { 综合条件 }
    WEEK_COND1 AND DAY_COND_1 AND DAY_COND_2;
    """

    def __init__(self, params: Dict[str, Any] = None):
        super().__init__(params)
        default_params = {
            'buy_size': 100,  # 每次买入股数
            'take_profit': 0.1,  # 止盈比例 20%
            'stop_loss': 0.1,  # 止损比例 5%
            'max_positions': 20  # 每只股票最大持仓笔数
        }
        if params:
            default_params.update(params)
        super().__init__(default_params)

        self.params = default_params

    def run_simulation(self, data_dict: Dict[str, pd.DataFrame], time_index: pd.DatetimeIndex) -> Tuple[
        Any, Dict[str, Any]]:
        """
        run_simulation
        """
        week_data_dict = merge_data(data_dict, '1W')
        day_data_dict = merge_data(data_dict, '1D')

        ema13 = data_utils.EMA(week_data_dict['close'], 13)
        ema21 = data_utils.EMA(week_data_dict['close'], 21)
        ema34 = data_utils.EMA(week_data_dict['close'], 34)
        week_cond = (ema13 > ema21) & (ema21 > ema34)
        del ema13, ema21, ema34

        # 特殊处理，将前34根k线设置为False
        week_cond.iloc[:34] = False

        # 下放至日线级别
        week_cond = week_cond.reindex(day_data_dict['close'].index, method='bfill')

        # 日线条件
        ema13 = data_utils.EMA(day_data_dict['close'], 13)
        ema21 = data_utils.EMA(day_data_dict['close'], 21)
        ema34 = data_utils.EMA(day_data_dict['close'], 34)
        ema55 = data_utils.EMA(day_data_dict['close'], 55)
        day_cond_1 = (ema13 > ema21) & (ema21 > ema55)
        day_cond_2 = data_utils.CROSS(ema21, ema34)
        del ema13, ema21, ema34, ema55

        # 合成买入信号
        entry_signal = day_cond_1 & day_cond_2
        entry_signal[:55] = False
        del day_cond_1, day_cond_2

        # 下放至分钟线级别
        minute_index_normalized = data_dict['close'].index.normalize()
        entry_signal_normalized = entry_signal.copy()
        entry_signal_normalized.index = pd.to_datetime(entry_signal_normalized.index).normalize()
        # 去除重复索引，保留最后一个值
        entry_signal_normalized = entry_signal_normalized[~entry_signal_normalized.index.duplicated(keep='last')]
        entry_signal = entry_signal_normalized.reindex(minute_index_normalized, method='bfill')
        entry_signal.index = data_dict['close'].index

        # 获取参数
        buy_size = self.params.get('buy_size', 100)
        take_profit = self.params.get('take_profit', 0.2)
        stop_loss = self.params.get('stop_loss', 0.05)
        max_positions = self.params.get('max_positions', 20)

        # 生成日期索引数组（用于限制每天只能买入一次）
        # 将时间戳转换为日期，再转换为整数索引
        dates = data_dict['close'].index.normalize()
        unique_dates = dates.unique()
        date_to_idx = {d: i for i, d in enumerate(unique_dates)}
        day_indices = np.array([date_to_idx[d] for d in dates], dtype=np.int16)

        # 生成卖出信号
        entry_signal_values = entry_signal.values
        close_values = data_dict['close'].values.astype(np.float64)
        exit_signal_values = numba_generate_fibonacci_ema_multi_position_signals(
            entry_signal_values, close_values,
            day_indices,
            buy_size=buy_size,
            take_profit=take_profit,
            stop_loss=stop_loss,
            max_positions=max_positions
        )
        exit_signal = pd.DataFrame(exit_signal_values, index=data_dict['close'].index,
                                   columns=data_dict['close'].columns)

        return TRADE_METHOD.BUY_AND_SELL_SIGNALS, {"entries": entry_signal, "exits": exit_signal}
