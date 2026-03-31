import pandas as pd
import numpy as np
from typing import Dict
import datetime
from numpy.polynomial.polynomial import polyfit, polyval


def get_single_kind_data(data: Dict[str, pd.DataFrame], kind: str) -> pd.DataFrame:
    """
    从字典中提取指定列的数据，生成新的 DataFrame。
    内存优化版本：避免重复复制数据，使用更高效的合并策略。

    Args:
        data: 字典，键为 symbol，值为包含 date 和其他列的 DataFrame
        kind: 要提取的列名（如 'open', 'close', 'volume' 等）

    Returns:
        pd.DataFrame: index 为日期，columns 为 symbol，values 为 kind 列的数据

    Raises:
        ValueError: 如果输入字典为空、kind 列不存在或 date 格式不正确
    """
    if not data:
        raise ValueError("Input dictionary is empty")

    # 内存优化：收集所有数据到字典中，然后一次性构建DataFrame
    result_data = {}
    all_dates = set()

    # 第一遍：收集所有数据和日期
    for symbol, df in data.items():
        # 验证 DataFrame 是否包含 date 和 kind 列
        if 'date' not in df.columns or kind not in df.columns:
            raise ValueError(f"DataFrame for {symbol} missing 'date' or '{kind}' column")

        # 确保 date 列为 datetime 格式 - 避免复制整个DataFrame
        try:
            # 只处理需要的列，避免复制整个DataFrame，并过滤掉无效日期
            date_series = pd.to_datetime(df['date'])

            # 过滤掉 NaT (Not a Time) 值
            valid_mask = date_series.notna()
            if not valid_mask.any():
                raise ValueError(f"DataFrame for {symbol} has no valid dates")

            # 只保留有效日期的数据
            dates = date_series[valid_mask].dt.strftime('%Y-%m-%d')
            values = df[kind][valid_mask]
        except Exception as e:
            raise ValueError(f"Invalid date format in DataFrame for {symbol}: {str(e)}")

        # 创建该股票的数据字典
        symbol_data = dict(zip(dates, values))
        result_data[symbol] = symbol_data
        all_dates.update(dates)

    # 如果没有数据，返回空 DataFrame
    if not result_data:
        return pd.DataFrame()

    # 内存优化：将日期排序，避免后续排序操作
    sorted_dates = sorted(all_dates)

    # 内存优化：一次性构建最终DataFrame
    final_data = {}
    for symbol, symbol_dict in result_data.items():
        # 为每个symbol创建完整的时间序列，缺失值用NaN填充
        final_data[symbol] = [symbol_dict.get(date, np.nan) for date in sorted_dates]

    # 一次性创建最终的DataFrame
    result = pd.DataFrame(final_data, index=sorted_dates)
    result.index.name = 'date'

    # 确保索引是DatetimeIndex类型，以支持resample操作
    try:
        result.index = pd.to_datetime(result.index)
    except Exception as e:
        print(f"警告：无法将索引转换为DatetimeIndex: {e}")

    return result


def get_single_kind_data_original(data: Dict[str, pd.DataFrame], kind: str) -> pd.DataFrame:
    """
    原始版本的get_single_kind_data函数 - 保留作为备份
    """
    if not data:
        raise ValueError("Input dictionary is empty")

    # 初始化结果 DataFrame
    result = None

    # 遍历每个 symbol 的 DataFrame
    for symbol, df in data.items():
        # 验证 DataFrame 是否包含 date 和 kind 列
        if 'date' not in df.columns or kind not in df.columns:
            raise ValueError(f"DataFrame for {symbol} missing 'date' or '{kind}' column")

        # 确保 date 列为 datetime 格式
        try:
            df = df.copy()  # 避免修改原始数据
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        except Exception as e:
            raise ValueError(f"Invalid date format in DataFrame for {symbol}: {str(e)}")

        # 设置 date 为索引并提取 kind 列
        temp_df = df.set_index('date')[[kind]].rename(columns={kind: symbol})

        # 合并到结果 DataFrame
        if result is None:
            result = temp_df
        else:
            result = result.join(temp_df, how='outer')

    # 如果结果为空，返回空 DataFrame
    if result is None:
        return pd.DataFrame()

    # 确保列名正确并返回
    return result


def get_single_kind_data_batch(data: Dict[str, pd.DataFrame], kind: str, batch_size: int = 100) -> pd.DataFrame:
    """
    分批处理版本的get_single_kind_data，适用于超大数据集

    Args:
        data: 字典，键为 symbol，值为包含 date 和其他列的 DataFrame
        kind: 要提取的列名（如 'open', 'close', 'volume' 等）
        batch_size: 每批处理的股票数量

    Returns:
        pd.DataFrame: index 为日期，columns 为 symbol，values 为 kind 列的数据
    """
    if not data:
        raise ValueError("Input dictionary is empty")

    import gc

    symbols = list(data.keys())
    total_symbols = len(symbols)
    all_dates = set()
    result_data = {}

    print(f"    分批处理 {total_symbols} 只股票的 {kind} 数据，每批 {batch_size} 只")

    # 分批处理股票
    for batch_start in range(0, total_symbols, batch_size):
        batch_end = min(batch_start + batch_size, total_symbols)
        current_batch = symbols[batch_start:batch_end]

        print(f"      处理第 {batch_start // batch_size + 1} 批 ({batch_start + 1}-{batch_end}/{total_symbols})")

        # 处理当前批次
        for symbol in current_batch:
            df = data[symbol]

            # 验证列是否存在
            if 'date' not in df.columns or kind not in df.columns:
                print(f"        警告: {symbol} 缺少 'date' 或 '{kind}' 列，跳过")
                continue

            try:
                # 只处理需要的列，并过滤掉无效日期
                date_series = pd.to_datetime(df['date'])

                # 过滤掉 NaT (Not a Time) 值
                valid_mask = date_series.notna()
                if not valid_mask.any():
                    print(f"        警告: {symbol} 没有有效的日期数据，跳过")
                    continue

                # 只保留有效日期的数据
                valid_dates = date_series[valid_mask].dt.strftime('%Y-%m-%d')
                valid_values = df[kind][valid_mask]

                # 创建该股票的数据字典
                symbol_data = dict(zip(valid_dates, valid_values))
                result_data[symbol] = symbol_data
                all_dates.update(valid_dates)

            except Exception as e:
                print(f"        错误: 处理股票 {symbol} 时发生异常: {e}")
                continue

        # 每批处理完后回收内存
        gc.collect()

    # 如果没有数据，返回空 DataFrame
    if not result_data:
        return pd.DataFrame()

    print(f"    构建最终的 {kind} 数据矩阵...")

    # 排序日期
    sorted_dates = sorted(all_dates)

    # 一次性构建最终DataFrame
    final_data = {}
    for symbol, symbol_dict in result_data.items():
        final_data[symbol] = [symbol_dict.get(date, np.nan) for date in sorted_dates]

    # 创建最终的DataFrame
    result = pd.DataFrame(final_data, index=sorted_dates)
    result.index.name = 'date'

    # 确保索引是DatetimeIndex类型，以支持resample操作
    try:
        result.index = pd.to_datetime(result.index)
    except Exception as e:
        print(f"    警告：无法将索引转换为DatetimeIndex: {e}")

    # 清理临时数据
    del result_data, all_dates, final_data
    gc.collect()

    print(f"    {kind} 数据构建完成，形状: {result.shape}")
    return result


def compose_bar(data: pd.DataFrame, frequency: str, kind: str) -> pd.DataFrame:
    """
    将数据按频率组合成K线

    Args:
        data: 形如:
                symbol1 symbol2 symbol3
            date1  value1 value2 value3
            date2  value1 value2 value3
            date3  value1 value2 value3
        frequency: 频率，允许有 'W'(周线), 'M'(月线), 'Q'(季线), 'Y'(年线)
        kind: 表示这个data的数据种类（open,close,high,low,volume,amount），便于后续计算

    Returns:
        pd.DataFrame: index 为日期，columns 为 symbol，values 为 kind 列的数据
    """
    # 确保索引是DatetimeIndex类型
    if not isinstance(data.index, pd.DatetimeIndex):
        try:
            # 尝试将索引转换为datetime格式
            data = data.copy()
            data.index = pd.to_datetime(data.index)
        except Exception as e:
            raise ValueError(f"无法将索引转换为DatetimeIndex类型: {e}")

    # 根据数据类型选择相应的聚合方式
    match kind:
        case 'open':
            return data.resample(frequency, label='right').first()
        case 'close':
            return data.resample(frequency, label='right').last()
        case 'high':
            return data.resample(frequency, label='right').max()
        case 'low':
            return data.resample(frequency, label='right').min()
        case 'volume':
            return data.resample(frequency, label='right').sum()
        case 'amount':
            return data.resample(frequency, label='right').sum()
        case _:
            # 默认情况，对于其他类型的数据使用last()
            return data.resample(frequency, label='right').last()


def compose_bar_example(symbol, start_time, end_time, frequency):
    '''symbol:标的，start_time:开始时间，end_time:结束时间，frequency:频率'''
    # 合成周线
    if frequency == 'W':
        # 判断开始日期是否为周一（默认周一 = 0，周二 = 1，所以对得出的week进行调整，加一）
        start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
        week_start = start_time.weekday() + 1
        # 如果不是周一，则将开始时间调整到该周周一
        if week_start != 1:
            print('输入的开始日期为周{}，调整到该周周一'.format(week_start))
            start_time = start_time - datetime.timedelta(days=week_start - 1)

    # 合成月线
    if frequency == 'M':
        # 判断开始日期是否为月初
        day_start_1th = int(start_time[8:10])
        # 如果不是1号，则转为当月1号
        if day_start_1th != 1:
            start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
            start_time = start_time - datetime.timedelta(days=day_start_1th - 1)
            print('开始日期为该月{}号，调整到当月1号', format(day_start_1th))

    # 合成季线
    if frequency == 'Q':
        # 直接开始日期设置为季度初
        month = int(start_time[5:7])  # 看开始时间月份
        year = int(start_time[0:4])  # 看开始时间年份
        if month in range(1, 3):
            start_time = datetime.date(year, 1, 1)
        if month in range(4, 6):
            start_time = datetime.date(year, 4, 1)
        if month in range(7, 9):
            start_time = datetime.date(year, 7, 1)
        if month in range(10, 12):
            start_time = datetime.date(year, 9, 1)

    # 合成年线
    if frequency == 'A':
        # 将开始时间调整为年初
        year_start = int(start_time[0:4])  # 看开始时间年份
        start_time = datetime.date(year_start, 1, 1)

    # 订阅历史数据（注意要复权到当前日期才能和新浪财经的数据对得上）
    data = history(symbol=symbol, frequency='1d', start_time=start_time, end_time=end_time,
                   fields='eob,open,close,high,low,amount,volume', df=True,
                   skip_suspended=True, fill_missing=None, adjust=ADJUST_PREV,
                   adjust_end_time=datetime.datetime.today())
    # 修改日期格式并变成索引
    data.eob = data.eob.apply(lambda x: datetime.datetime.strptime(str(x).split(' ')[0], '%Y-%m-%d'))
    data.set_index(data['eob'], inplace=True)
    data.drop(columns=['eob'], inplace=True)
    data_index = data.resample(frequency, label='right').last().index
    # 结果返回成dataframe格式
    data_k = pd.DataFrame({'open': data.resample(frequency, label='right').first()['open'],
                           'close': data.resample(frequency, label='right').last()['close'],
                           'high': data.resample(frequency, label='right').max()['high'],
                           'low': data.resample(frequency, label='right').min()['low'],
                           'amount': data.resample(frequency, label='right').sum()['amount'],
                           'volume': data.resample(frequency, label='right').sum()['volume'],
                           'frequency': frequency
                           })
    data_k.set_index(data_index, inplace=True)

    # 如果某周放假没有数据，则删除
    data_k.dropna(inplace=True)

    return data_k


# -------------------------------------------------------------------- 通达信函数 --------------------------------------------------------------------
def REF(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    获取n个周期前的值
    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    return data.shift(n)


def MA(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    求X的N日移动平均值
    算法是：(X1+X2+X3+…..+Xn)/N

    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    return data.rolling(window=n).mean()


def EMA(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    求X的N日指数平滑移动平均
    算法：若Y=EMA(X，N)，则Y=〔2*X+(N-1)*Y'〕/(N+1)
    其中Y'表示上一周期的Y值，2是平滑系数

    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    return data.ewm(alpha=2 / (n + 1), adjust=False).mean()


def SMA(data: pd.DataFrame, n: int, m: int) -> pd.DataFrame:
    """
    X的M日加权移动平均，M为权重
    算法：Y=(X*M+Y'*(N-M))/N
    其中Y'表示上一周期Y值，要求M < N

    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    n: 周期数
    m: 权重（必须小于n）
    """
    alpha = m / n
    return data.ewm(alpha=alpha, adjust=False).mean()


def HHV(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    获取n个周期的最高值
    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    return data.rolling(window=n).max().bfill()


def LLV(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    获取n个周期的最低值
    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    return data.rolling(window=n).min().bfill()


def DMA(data: pd.DataFrame, a) -> pd.DataFrame:
    """
    求X的动态移动平均，A为动态因子
    算法：若Y=DMA(X，A)则 Y=A*X+(1-A)*Y'
    其中Y'表示上一周期Y值，A必须小于1

    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3

    a: 动态因子，可以是常数或DataFrame（与data同形状）

    性能优化：使用 NumPy 向量化替代 Python 逐行循环，
    对每列独立计算递推，避免 DataFrame.iloc 的巨大开销。
    """
    # 转换为 numpy 数组进行计算，避免 pandas 索引开销
    data_np = data.values.astype(np.float64)
    n_rows, n_cols = data_np.shape

    if isinstance(a, (int, float)):
        weight_np = np.full_like(data_np, a, dtype=np.float64)
    elif isinstance(a, pd.DataFrame):
        weight_np = a.values.astype(np.float64)
    else:
        raise TypeError("Argument 'A' must be a float or a pandas DataFrame.")

    # 使用 numpy 数组进行递推计算（逐行，但操作的是整行向量，而非 DataFrame.iloc）
    result_np = np.empty_like(data_np, dtype=np.float64)
    result_np[0] = data_np[0]

    for i in range(1, n_rows):
        result_np[i] = weight_np[i] * data_np[i] + (1.0 - weight_np[i]) * result_np[i - 1]

    return pd.DataFrame(result_np, index=data.index, columns=data.columns)


def XMA(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    XMA函数的准确实现 - 改进的居中移动平均
    基于通达信XMA算法，使用渐变窗口处理边界

    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    # 输入验证
    if data.empty:
        raise ValueError("输入 DataFrame 为空")
    if n <= 0:
        raise ValueError("周期 n 必须为正整数")

    def _xma_single_column(src: np.ndarray, N: int) -> np.ndarray:
        """对单列数据应用XMA算法"""
        data_len = len(src)
        half_len = (N // 2) + (1 if N % 2 else 0)

        if data_len < half_len:
            return np.array([np.nan for i in range(data_len)], dtype=float)

        def _ma(arr, period):
            return pd.Series(arr).rolling(window=period).mean().values

        # Head部分：使用渐增窗口
        head = np.array([_ma(src[0:ilen], ilen)[-1] for ilen in range(half_len, N)])
        out = head

        if data_len >= N:
            # Body部分：使用固定窗口N
            body = _ma(src, N)[N - 1:]
            out = np.append(out, body)

            # Tail部分：使用渐减窗口
            tail = np.array([_ma(src[-ilen:], ilen)[-1] for ilen in range(N - 1, half_len - 1, -1)])
            out = np.append(out, tail)

        return out

    # 对DataFrame的每一列应用XMA算法
    result_data = {}
    for column in data.columns:
        src_array = data[column].values.astype(float)
        xma_result = _xma_single_column(src_array, n)
        result_data[column] = xma_result

    # 创建结果DataFrame，保持原有的索引
    result = pd.DataFrame(result_data, index=data.index)

    return result


def LSMA(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    最小二乘移动平均线 (线性回归预测)
    对过去n个周期的数据拟合一条回归线，并返回该线在当前时刻的预测值。
    这是一个无未来函数的高级移动平均。
    对过去 n 个周期的数据拟合一条最小二乘法线性回归线 (y = mx + c)。然后，用这条线的终点值（即当前 T 时刻对应的回归线上的值）作为当前的LSMA值。如果趋势持续，这个值就是对当前价格的最好估计
    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    if data.empty:
        raise ValueError("输入 DataFrame 为空")
    if n <= 1:
        raise ValueError("周期 n 必须大于1")

    # 定义用于 rolling.apply 的单列计算函数
    def _linreg(y: np.ndarray) -> float:
        """对一个窗口的数据计算线性回归预测值"""
        if len(y) < n:
            return np.nan
        # 创建 x 轴 (0, 1, 2, ..., n-1)
        x = np.arange(len(y))
        # 拟合线性回归 (c, m) = (截距, 斜率)
        c, m = polyfit(x, y, 1)
        # 返回回归线在最后一个点 (x=n-1) 的值
        return m * (n - 1) + c

    # 对每一列应用 rolling 和 _linreg 函数
    result = data.apply(lambda col: col.rolling(window=n).apply(_linreg, raw=True))
    return result


def DEMA(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    双重指数移动平均线 DEMA
    通过结合两次EMA计算来显著减少滞后。
    DEMA 不是简单地对价格进行一次EMA计算，而是通过对“EMA(价格)”再次进行EMA计算，并结合一个数学公式来消除部分滞后。公式为: DEMA = 2 * EMA(n) - EMA(EMA(n))

    data: 形如:
        symbol1 symbol2 symbol3
    date1  value1 value2 value3
    date2  value1 value2 value3
    date3  value1 value2 value3
    """
    if data.empty:
        raise ValueError("输入 DataFrame 为空")

    ema1 = data.ewm(span=n, adjust=False).mean()
    ema2 = ema1.ewm(span=n, adjust=False).mean()

    dema = 2 * ema1 - ema2
    return dema


def VWMA(price_data: pd.DataFrame, volume_data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    计算成交量加权移动平均 (Volume Weighted Moving Average) - 完全无未来函数。

    VWMA = Sum(Price * Volume) / Sum(Volume) over a rolling window.

    Args:
        price_data (pd.DataFrame): 价格数据 (通常是收盘价)。
        volume_data (pd.DataFrame): 成交量数据。
        n (int): 移动平均的周期。

    Returns:
        pd.DataFrame: 计算后的VWMA结果。
    """
    # 输入验证
    if not price_data.index.equals(volume_data.index) or not price_data.columns.equals(volume_data.columns):
        raise ValueError("价格数据和成交量数据的形状或索引必须完全相同。")
    if n <= 0:
        raise ValueError("周期 n 必须为正整数")

    # 1. 计算分子：价格与成交量的乘积 (即每日的总成交额)
    # 在pandas中，price_data * volume_data 已经可以看作是成交额了
    turnover = price_data * volume_data

    # 2. 对成交额进行N日滚动求和
    sum_of_turnover = turnover.rolling(window=n, min_periods=1).sum()

    # 3. 对成交量进行N日滚动求和
    sum_of_volume = volume_data.rolling(window=n, min_periods=1).sum()

    # 4. 计算最终结果：总成交额 / 总成交量
    # 为避免除以零的错误（例如停牌日），将分母中为0的值替换为NaN
    sum_of_volume = sum_of_volume.replace(0, np.nan)

    vwma_result = sum_of_turnover / sum_of_volume

    return vwma_result


def SLOPE(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    求X的N日斜率
    """
    x_index = np.arange(n)

    return data.rolling(n).apply(
        lambda x: np.polyfit(x_index, x, 1)[0],
        raw=True
    )


def STD(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    求X的N日标准差
    """
    return data.rolling(window=n).std()


def IF(condition: pd.DataFrame, true_value, false_value) -> pd.DataFrame:
    """
    通达信 IF 函数：根据条件选择 true_value 或 false_value。

    Args:
        condition: 布尔型 DataFrame，决定选择 true_value 还是 false_value。
        true_value: 标量或与 condition 形状相同的 DataFrame。
        false_value: 标量或与 condition 形状相同的 DataFrame。

    Returns:
        与 condition 形状相同的 DataFrame，包含根据条件选择的值。
    """
    # 确保 condition 是布尔型 DataFrame
    if not isinstance(condition, pd.DataFrame):
        raise TypeError("condition must be a pandas DataFrame")

    # 使用 numpy.where 实现向量化 IF 操作
    result = np.where(condition, true_value, false_value)

    # 将结果转换为 DataFrame，保持原始的 index 和 columns
    return pd.DataFrame(result, index=condition.index, columns=condition.columns)


def MAX(a, b) -> pd.DataFrame:
    """
    通达信 MAX 函数：逐元素取 a 和 b 的最大值。

    Args:
        a: 标量或 DataFrame，至少有一个输入是 DataFrame。
        b: 标量或 DataFrame。

    Returns:
        与 DataFrame 输入形状相同的 DataFrame，包含逐元素最大值。
    """
    # 确保至少有一个输入是 DataFrame
    if not (isinstance(a, pd.DataFrame) or isinstance(b, pd.DataFrame)):
        raise TypeError("At least one of a or b must be a pandas DataFrame")

    # 如果 a 或 b 是标量，将其广播为与 DataFrame 形状相同的数组
    if isinstance(a, pd.DataFrame):
        result = a.copy()  # 复制 DataFrame 以保留 index 和 columns
        result = np.maximum(result, b)
    else:
        result = b.copy()
        result = np.maximum(a, result)

    # 确保返回的是 DataFrame
    if not isinstance(result, pd.DataFrame):
        result = pd.DataFrame(result, index=b.index if isinstance(b, pd.DataFrame) else a.index,
                              columns=b.columns if isinstance(b, pd.DataFrame) else a.columns)

    return result


def MIN(a, b) -> pd.DataFrame:
    """
    通达信 MIN 函数：逐元素取 a 和 b 的最小值。

    Args:
        a: 标量或 DataFrame，至少有一个输入是 DataFrame。
        b: 标量或 DataFrame。

    Returns:
        与 DataFrame 输入形状相同的 DataFrame，包含逐元素最小值。
    """
    # 确保至少有一个输入是 DataFrame
    if not (isinstance(a, pd.DataFrame) or isinstance(b, pd.DataFrame)):
        raise TypeError("At least one of a or b must be a pandas DataFrame")

    # 如果 a 或 b 是标量，将其广播为与 DataFrame 形状相同的数组
    if isinstance(a, pd.DataFrame):
        result = a.copy()  # 复制 DataFrame 以保留 index 和 columns
        result = np.minimum(result, b)
    else:
        result = b.copy()
        result = np.minimum(a, result)

    # 确保返回的是 DataFrame
    if not isinstance(result, pd.DataFrame):
        result = pd.DataFrame(result, index=b.index if isinstance(b, pd.DataFrame) else a.index,
                              columns=b.columns if isinstance(b, pd.DataFrame) else a.columns)

    return result


def COUNT(condition: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    通达信 COUNT 函数：统计满足条件的周期数。
    Args:
        condition: 布尔型 DataFrame。
        n: 周期数。

    Returns:
        与 condition 形状相同的 DataFrame，包含满足条件的周期数。
    """
    return condition.rolling(window=n).sum().fillna(0)


def EVERY(data: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    通达信 EVERY 函数：连续N周期满足条件
    """
    # 将布尔值转为 1/0，然后用滚动求和判断是否等于周期N
    return data.astype(int).rolling(window=n).sum() == n


def EXIST(condition: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    通达信 EXISTS 函数：判断是否存在满足条件的周期
    """
    return condition.astype(int).rolling(window=n).sum() > 0


def BARSLAST(condition: pd.DataFrame) -> pd.DataFrame:
    """
    通达信 BARSLAST 函数的向量化 DataFrame 实现。

    计算每个时间点距离上一次条件成立到当前的周期数。
    如果条件在当前周期成立，则结果为 0。
    如果条件在之前从未成立过，则结果为 NaN。

    Args:
        condition: 一个布尔型的 DataFrame，其中 True 代表条件成立。

    Returns:
        一个与 condition 形状相同的 DataFrame，数据类型为 float，
        包含每个点距离上次条件成立的周期数。
    """
    # 性能优化：使用 dtypes 检查替代逐元素 map(lambda)，速度提升百倍以上
    if not all(dt == bool or dt == np.bool_ for dt in condition.dtypes):
        condition = condition.astype(bool)

    # 核心逻辑：使用 cumsum 创建分组ID，然后对每个组进行 cumcount
    # 这个技巧可以巧妙地为每个“条件成立”后的周期序列进行计数
    def _barslast_for_series(col: pd.Series) -> pd.Series:
        """对单个Series应用BARSLAST逻辑"""
        # 1. 使用 cumsum() 创建分组ID。每当遇到一个True，ID就会加1。
        #    这有效地将Series分成了多个块，每个块从一个True开始。
        group_ids = col.cumsum()

        # 2. 对每个分组进行 cumcount()。这将为每个分组内的元素从0开始编号。
        #    这个编号正好是“距离组开始（即上一个True）的周期数”。
        counts = col.groupby(group_ids).cumcount()

        # 3. 处理特殊情况：在第一个True出现之前的周期。
        #    这些周期的 group_id 都是 0。因为它们之前没有True，
        #    所以 BARSLAST 的结果应该是未定义的（NaN）。
        counts[group_ids == 0] = np.nan

        return counts

    # 将上述逻辑应用到输入DataFrame的每一列
    result = condition.apply(_barslast_for_series)

    return result.fillna(0)


def CROSS(a, b) -> pd.DataFrame:
    """
    通达信 CROSS 函数：判断 a 上穿 b（a 从下方穿过 b）。

    支持的参数类型：
    - a, b 都是 DataFrame
    - a 是 DataFrame, b 是常数 (int/float)
    - a 是常数 (int/float), b 是 DataFrame
    """
    # 判断是否为常数
    a_is_scalar = isinstance(a, (int, float))
    b_is_scalar = isinstance(b, (int, float))

    # 获取当前值和前一时刻值
    a_curr = a
    a_prev = a if a_is_scalar else a.shift(1)
    b_curr = b
    b_prev = b if b_is_scalar else b.shift(1)

    return (a_curr > b_curr) & (a_prev <= b_prev)


def BARSCOUNT(condition: pd.DataFrame) -> pd.DataFrame:
    """
    通达信 BARSCOUNT 函数的向量化 DataFrame 实现。

    计算每个时间点条件连续成立的周期数。
    如果当天条件不成立，则计数归零。

    Args:
        condition: 一个布尔型的 DataFrame，其中 True 代表条件成立。

    Returns:
        一个与 condition 形状相同的 DataFrame，数据类型为 int，
        包含每个点条件连续成立的周期数。
    """
    # 性能优化：使用 dtypes 检查替代逐元素 map(lambda)，速度提升百倍以上
    if not all(dt == bool or dt == np.bool_ for dt in condition.dtypes):
        condition = condition.astype(bool)

    def _barscount_for_series(col: pd.Series) -> pd.Series:
        """对单个Series应用BARSCOUNT逻辑"""
        # 1. 对条件的否定 (!) 使用 cumsum() 创建分组ID。
        #    每当遇到一个False (即 ~col 为 True)，分组ID就会加1。
        #    这有效地将Series按False的位置切分开。
        group_ids = (~col).cumsum()

        # 2. 将原条件 (col) 转为整数 (True=1, False=0)。
        # 3. 按刚刚创建的 group_ids 分组，并在每个组内进行 cumsum()。
        #    - 在一个由连续True组成的组内，cumsum的结果是 [1, 2, 3, ...]
        #    - 在一个由False开始的组内，cumsum的结果是 [0, 1, 2, ...]，但由于False本身是0，所以结果正确。
        counts = col.astype(int).groupby(group_ids).cumsum()
        return counts

    # 将上述逻辑应用到输入DataFrame的每一列
    result = condition.apply(_barscount_for_series)

    return result.astype(int)


def SUM(series: pd.DataFrame, n: pd.DataFrame) -> pd.DataFrame:
    """
    通达信 SUM(X, N) 函数的向量化 DataFrame 实现，支持动态周期。

    计算 X 在过去 N 个周期内的总和，其中 N 可以是 DataFrame，
    为每个数据点指定不同的求和周期。

    Args:
        series: 一个数值型的 DataFrame (对应 X)。
        n: 一个整数型的 DataFrame (对应 N)，其形状必须与 series 相同。
           其中所有值必须大于 0。

    Returns:
        一个与 series 形状相同的 DataFrame，包含动态周期的滚动求和结果。

    Raises:
        ValueError: 如果 n 和 series 的形状不匹配，或者 n 包含非正数值。
    """
    # --- 1. 输入验证 ---
    if series.shape != n.shape:
        raise ValueError(f"输入 'series' 和 'n' 的形状必须相同。 "
                         f"series shape: {series.shape}, n shape: {n.shape}")

    # astype(int) is used to ensure comparison works even if n is float
    if (n.astype(int) <= 0).any().any():
        raise ValueError("周期 'n' DataFrame 中的所有值都必须大于 0。")

    # --- 2. 准备 NumPy 数组 ---
    series_np = series.to_numpy()
    n_np = n.astype(int).to_numpy()

    # --- 3. 计算累积和 ---
    # 这是我们算法的核心，后续所有求和都基于这个预计算的结果
    cum_sum_np = np.cumsum(series_np, axis=0)

    # --- 4. 计算动态的回溯索引 ---
    # 创建一个表示当前行号的数组
    rows = np.arange(series_np.shape[0])[:, np.newaxis]

    # 对于每个点 (r, c)，我们要查找的起始索引是 r - n[r, c]
    # 这是 Cumsum(series)[i - n[i]] 中的 `i - n[i]` 部分
    lookup_indices = rows - n_np

    # --- 5. 执行动态查找并计算结果 ---
    # 我们需要从 cum_sum_np 中减去回溯点的累积和
    # 初始化一个与原始数据形状相同的零数组，用于存放要减去的值
    subtrahend = np.zeros_like(series_np, dtype=float)

    # 创建一个掩码，只对有效的（非负的）回溯索引进行操作
    valid_mask = lookup_indices >= 0

    # 获取需要执行查找操作的行和列的索引
    valid_rows, valid_cols = np.where(valid_mask)

    # 获取在这些有效位置上，我们需要去 cum_sum_np 中查找的具体行号
    lookup_rows = lookup_indices[valid_mask]

    # 使用高级索引，一次性从 cum_sum_np 中提取所有需要减去的值
    # 对于 (valid_rows[k], valid_cols[k]) 这个位置,
    # 我们要减去的值是 cum_sum_np 在 (lookup_rows[k], valid_cols[k]) 的值
    subtrahend[valid_mask] = cum_sum_np[lookup_rows, valid_cols]

    # 最终结果 = 当前点的累积和 - 回溯点的累积和
    result_np = cum_sum_np - subtrahend

    # --- 6. 转换回 DataFrame ---
    return pd.DataFrame(result_np, index=series.index, columns=series.columns)


def BIAS(data: pd.DataFrame, n: int):
    return (data - MA(data, n)) / MA(data, n) * 100


def CCI(close: pd.DataFrame, high: pd.DataFrame, low: pd.DataFrame, n: int = 14):
    tp = (high + low + close) / 3
    ma = tp.rolling(n).mean()
    # 性能优化：使用 raw=True 让 pandas 传入 numpy 数组而非 Series，
    # 配合纯 numpy 操作，速度提升数倍
    md = tp.rolling(n).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))),
        raw=True
    )
    cci = (tp - ma) / (0.015 * md)
    return cci