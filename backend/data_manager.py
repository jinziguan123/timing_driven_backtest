import os
import mmap
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Iterable, Optional, List, Dict

try:
    from customized_backtest_tool.backend.clickhouse_bar_reader import ClickHouseBarReader
    from customized_backtest_tool.backend.clickhouse_bar_storage import ClickHouseBarStorage
    from customized_backtest_tool.backend.mysql_bar_common import normalize_symbol_bar_frame
    from customized_backtest_tool.backend.mysql_bar_reader import MysqlBarReader
    from customized_backtest_tool.backend.qfq_factor_reader import QfqFactorReader
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage
except ModuleNotFoundError:
    from clickhouse_bar_reader import ClickHouseBarReader
    from clickhouse_bar_storage import ClickHouseBarStorage
    from mysql_bar_common import normalize_symbol_bar_frame
    from mysql_bar_reader import MysqlBarReader
    from qfq_factor_reader import QfqFactorReader
    from mysql_bar_storage import MysqlBarStorage

# 强制所有浮点数显示为 2 位小数
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# 定义 dtype (保持不变)
IQUANT_DTYPE = np.dtype([
    ('time', '<u4'),
    ('open', '<i4'),
    ('high', '<i4'),
    ('low', '<i4'),
    ('close', '<i4'),
    ('unused_amt', '<f4'),
    ('volume', '<i4'),
    ('unused_res', '<i4'),
    ('amount', '<i8'),
    ('padding', 'V24')
])

QFQ_FACTOR_DF = None

# 允许通过环境变量覆盖数据目录，避免写死 Windows 路径
DEFAULT_LOCAL_DATA_DIR = r"C:\iQuant\国信iQuant策略交易平台\datadir"
LOCAL_DATA_DIR = os.environ.get("IQUANT_LOCAL_DATA_DIR", DEFAULT_LOCAL_DATA_DIR)
QFQ_FACTOR_DIR = os.environ.get("QFQ_FACTOR_DIR", r"C:\Users\18917\Desktop\merged_adjust_factors.parquet")

# 数据库配置（可通过环境变量配置）
DB_HOST = os.environ.get("DB_HOST", "10.10.1.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ainjin123")
DB_NAME = os.environ.get("DB_NAME", "stock_data")
DB_CHARSET = os.environ.get("DB_CHARSET", "utf8mb4")

# 数据库连接字符串
DB_CONNECTION_STRING = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset={DB_CHARSET}"


def _get_mysql_reader(mysql_reader=None):
    return mysql_reader or MysqlBarReader()


def _get_mysql_storage(mysql_storage=None):
    return mysql_storage or MysqlBarStorage()


def _get_clickhouse_reader(clickhouse_reader=None):
    return clickhouse_reader or ClickHouseBarReader()


def _get_clickhouse_storage(clickhouse_storage=None):
    return clickhouse_storage or ClickHouseBarStorage()


def _get_qfq_factor_reader(qfq_factor_reader=None):
    return qfq_factor_reader or QfqFactorReader()


def _period_dir(period: str) -> str:
    if period == '1d':
        return '86400'
    if period == '1m':
        return '60'
    raise ValueError(f"Unsupported period: {period}")


def get_dat_file_path(stock: str, period: str = '1m', base_dir: Optional[str] = None) -> str:
    """根据股票代码与周期拼出 .DAT 路径。

    stock: '600519.SH'
    period: '1m' or '1d'

    目录结构约定：<base>/<market>/<period_dir>/<code>.DAT
    """
    base = base_dir or LOCAL_DATA_DIR
    if '.' not in stock:
        raise ValueError("stock code must be like '600519.SH'")
    stock_code, market_code = stock.split('.', 1)
    return os.path.join(base, f'{market_code}', _period_dir(period), f'{stock_code}.DAT')


def read_iquant_mmap(
        file_path,
        fields: List[str] = ['open', 'high', 'low', 'close', 'volume', 'amount'],
        start_ts=None,
        end_ts=None,
):
    """使用 mmap (内存映射) 极省内存地读取数据"""
    if not os.path.exists(file_path):
        return pd.DataFrame()

    filtered_data = None

    try:
        with open(file_path, 'rb') as f:
            # 必须检查文件是否为空，否则 mmap 会报错
            f.seek(0, 2)
            if f.tell() < 8:
                return pd.DataFrame()
            f.seek(0, 0)

            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                # 1. 创建视图 (此时不占物理内存)
                data = np.frombuffer(mm, dtype=IQUANT_DTYPE, offset=8)

                # 2. 构建掩码 (Valid Mask)
                if start_ts is None and end_ts is None:
                    valid_mask = data['time'] > 0
                else:
                    valid_mask = data['time'] > 0
                    if start_ts:
                        valid_mask &= (data['time'] >= start_ts)
                    if end_ts:
                        valid_mask &= (data['time'] <= end_ts)

                # 3. 使用 Boolean Indexing 触发拷贝，脱离 mmap 控制
                filtered_data = data[valid_mask].copy()

                # 4. 显式删除视图，释放 mmap 引用
                del data

    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

    if filtered_data is None or filtered_data.size == 0:
        return pd.DataFrame()

    result = {}

    # 时间处理
    timestamps = filtered_data['time'].astype(np.int64)
    timestamps = (timestamps // 60) * 60
    timestamps += 28800
    index_dates = pd.to_datetime(timestamps, unit='s')

    # 使用 float32 节省内存
    if 'open' in fields:
        result['open'] = (filtered_data['open'] / 1000.0).astype(np.float32)
    if 'high' in fields:
        result['high'] = (filtered_data['high'] / 1000.0).astype(np.float32)
    if 'low' in fields:
        result['low'] = (filtered_data['low'] / 1000.0).astype(np.float32)
    if 'close' in fields:
        result['close'] = (filtered_data['close'] / 1000.0).astype(np.float32)

    if 'volume' in fields:
        result['volume'] = filtered_data['volume'].astype(np.float32)

    if 'amount' in fields:
        result['amount'] = (filtered_data['amount']).astype(np.float32)

    df = pd.DataFrame(result, index=index_dates)
    df.index.name = 'datetime'

    if not df.index.is_unique:
        df = df[~df.index.duplicated(keep='last')]

    return df


def fast_fill_missing_bars(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    unique_dates = df.index.normalize().unique()
    if len(unique_dates) == 0:
        return df

    am_offsets = np.arange(571, 691)
    pm_offsets = np.arange(781, 901)
    day_minutes = np.concatenate([am_offsets, pm_offsets])

    base_dates = unique_dates.values.astype('datetime64[m]')
    full_timestamps = base_dates[:, None] + day_minutes[None, :]
    full_index = pd.DatetimeIndex(full_timestamps.ravel())

    df_reindexed = df.reindex(full_index)

    df_reindexed['close'] = df_reindexed['close'].ffill()
    for col in ['open', 'high', 'low']:
        if col in df_reindexed.columns:
            df_reindexed[col] = df_reindexed[col].fillna(df_reindexed['close'])

    if 'volume' in df_reindexed.columns:
        df_reindexed['volume'] = df_reindexed['volume'].fillna(0)
    if 'amount' in df_reindexed.columns:
        df_reindexed['amount'] = df_reindexed['amount'].fillna(0)

    return df_reindexed


def _normalize_adjust_mode(adjust: Optional[str]) -> str:
    if adjust is None:
        return 'none'
    normalized = str(adjust).strip().lower()
    if normalized in {'', 'none', 'raw'}:
        return 'none'
    if normalized == 'qfq':
        return 'qfq'
    raise ValueError(f"Unsupported adjust mode: {adjust}")


def build_qfq_factor_map(
        stock_list: Iterable[str],
        start_date_time: Optional[str] = None,
        end_date_time: Optional[str] = None,
        qfq_factor_reader=None,
) -> Dict[str, pd.Series]:
    symbols = list(dict.fromkeys(str(stock).strip().upper() for stock in stock_list if stock))
    if not symbols:
        return {}
    reader = _get_qfq_factor_reader(qfq_factor_reader)
    start_date = pd.Timestamp(start_date_time).normalize() if start_date_time else None
    end_date = pd.Timestamp(end_date_time).normalize() if end_date_time else None
    return reader.load_factor_map(symbols, start_date=start_date, end_date=end_date)


def apply_qfq_to_symbol_frame(df: pd.DataFrame, factor_series: Optional[pd.Series]) -> pd.DataFrame:
    """在单只股票分钟线层面应用前复权，避免宽表级大矩阵。"""
    if df.empty:
        return df

    adjusted = df.copy()
    if factor_series is None or len(factor_series) == 0:
        return adjusted

    if not isinstance(adjusted.index, pd.DatetimeIndex):
        adjusted.index = pd.to_datetime(adjusted.index)

    event_series = pd.Series(factor_series).dropna()
    if event_series.empty:
        return adjusted

    event_series.index = pd.to_datetime(event_series.index).normalize()
    event_series = event_series.groupby(level=0).last().sort_index()

    price_dates_v = adjusted.index.values.astype('datetime64[D]')
    unique_dates_v = np.unique(price_dates_v)
    if len(unique_dates_v) == 0:
        return adjusted

    expanded_events = event_series.reindex(pd.DatetimeIndex(unique_dates_v)).fillna(1.0)
    cum_factor = expanded_events.cumprod()
    final_cum = float(cum_factor.iloc[-1])
    if not np.isfinite(final_cum) or final_cum == 0:
        return adjusted

    qfq_ratio_daily = (cum_factor / final_cum).to_numpy(dtype=np.float32)
    day_indices = np.searchsorted(unique_dates_v, price_dates_v)
    minute_ratios = qfq_ratio_daily[day_indices]

    for field in ['open', 'high', 'low', 'close']:
        if field not in adjusted.columns:
            continue
        field_values = adjusted[field].to_numpy(dtype=np.float32, copy=True)
        adjusted[field] = field_values * minute_ratios

    return adjusted


def load_data_generator(
        stock_list: Iterable[str] = (),
        fields: List[str] = ['open', 'high', 'low', 'close', 'volume', 'amount'],
        period: str = '1m',
        start_date_time: Optional[str] = None,
        end_date_time: Optional[str] = None,
        source: str = 'dat',
        adjust: Optional[str] = 'none',
        mysql_reader=None,
        clickhouse_reader=None,
        qfq_factor_reader=None,
        qfq_factor_map: Optional[Dict[str, pd.Series]] = None,
):
    """生成器模式：一次只返回一个 df，用完即丢"""
    start_ts = None
    end_ts = None
    normalized_source = str(source).strip().lower()
    normalized_adjust = _normalize_adjust_mode(adjust)
    normalized_stock_list = [str(stock).strip().upper() for stock in stock_list if stock]
    if start_date_time:
        start_ts = int(datetime.strptime(start_date_time, "%Y-%m-%d %H:%M:%S").timestamp())
    if end_date_time:
        end_ts = int(datetime.strptime(end_date_time, "%Y-%m-%d %H:%M:%S").timestamp())

    if normalized_adjust == 'qfq' and qfq_factor_map is None:
        qfq_factor_map = build_qfq_factor_map(
            normalized_stock_list,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            qfq_factor_reader=qfq_factor_reader,
        )

    for stock in normalized_stock_list:
        try:
            if normalized_source == 'dat':
                file_path = get_dat_file_path(stock, period=period)
                df = read_iquant_mmap(file_path, fields, start_ts, end_ts)
            elif normalized_source in {'mysql', 'clickhouse'}:
                reader = _get_mysql_reader(mysql_reader) if normalized_source == 'mysql' else _get_clickhouse_reader(clickhouse_reader)
                df = reader.load_symbol_minutes(
                    symbol=stock,
                    start_datetime=start_date_time,
                    end_datetime=end_date_time,
                )
                if fields and not df.empty:
                    existing_fields = [field for field in fields if field in df.columns]
                    df = df.loc[:, existing_fields]
            else:
                raise ValueError(f"Unsupported source: {source}")

            if not df.empty and period == '1m':
                df = fast_fill_missing_bars(df)

            if not df.empty and normalized_adjust == 'qfq':
                factor_series = (qfq_factor_map or {}).get(stock)
                df = apply_qfq_to_symbol_frame(df, factor_series)

            yield stock, df

        except Exception as e:
            print(f"Error loading {stock}: {e}")
            yield stock, pd.DataFrame()


def load_stock_minutes(
        stock: str,
        start_date_time: Optional[str] = None,
        end_date_time: Optional[str] = None,
        fields: List[str] = ['open', 'high', 'low', 'close', 'volume', 'amount'],
        source: str = 'dat',
        adjust: Optional[str] = 'none',
        mysql_reader=None,
        clickhouse_reader=None,
        qfq_factor_reader=None,
        qfq_factor_map: Optional[Dict[str, pd.Series]] = None,
) -> pd.DataFrame:
    """便捷方法：读取单只股票分钟线"""
    if source in {'mysql', 'clickhouse'}:
        reader = _get_mysql_reader(mysql_reader) if source == 'mysql' else _get_clickhouse_reader(clickhouse_reader)
        df = reader.load_symbol_minutes(
            symbol=stock,
            start_datetime=start_date_time,
            end_datetime=end_date_time,
        )
        if not df.empty and _normalize_adjust_mode(adjust) == 'qfq':
            factor_map = qfq_factor_map or build_qfq_factor_map(
                [stock],
                start_date_time=start_date_time,
                end_date_time=end_date_time,
                qfq_factor_reader=qfq_factor_reader,
            )
            df = apply_qfq_to_symbol_frame(df, factor_map.get(str(stock).strip().upper()))
        if fields:
            existing_fields = [field for field in fields if field in df.columns]
            return df.loc[:, existing_fields]
        return df

    if source != 'dat':
        raise ValueError(f"Unsupported source: {source}")

    for _, df in load_data_generator(
            stock_list=[stock],
            fields=fields,
            period='1m',
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            source='dat',
            adjust=adjust,
            qfq_factor_reader=qfq_factor_reader,
            qfq_factor_map=qfq_factor_map,
    ):
        return df
    return pd.DataFrame()


def prepare_vbt_data(generator, qfq_flag: bool = True) -> dict:
    """将 load_data_generator 的输出转换为 vectorbt 需要的字典格式"""
    global QFQ_FACTOR_DF  # 声明使用全局变量

    temp_data = {'open': {}, 'high': {}, 'low': {}, 'close': {}, 'volume': {}, 'amount': {}}

    print("正在将数据转换为 Vectorbt 矩阵格式...")

    for stock_code, df in generator:
        if df.empty:
            continue

        temp_data['open'][stock_code] = df['open']
        temp_data['high'][stock_code] = df['high']
        temp_data['low'][stock_code] = df['low']
        temp_data['close'][stock_code] = df['close']
        temp_data['volume'][stock_code] = df['volume']
        temp_data['amount'][stock_code] = df['amount']

    print("正在合并 DataFrame (这可能需要一些内存)...")

    final_data = {}
    for field, data_dict in temp_data.items():
        if not data_dict:
            continue
        df_panel = pd.DataFrame(data_dict).sort_index()
        final_data[field] = df_panel.astype(np.float32)

    if qfq_flag:
        if QFQ_FACTOR_DF is None:
            QFQ_FACTOR_DF = get_qfq_factor_from_file()
        final_data = calculate_qfq_data(final_data, QFQ_FACTOR_DF)

    print("数据准备完成。")
    return final_data


def merge_data(data_dict: dict, period: str = '1m', strict_mode: bool = False, 
               start_time: str = '09:30:00', end_time: str = '15:00:00'):
    """
    将数据合并为指定周期
    
    Args:
        data_dict: 数据字典，key 为指标名称，value 为 DataFrame
        period: 合并周期，如 '1m', '5m', '15m', '30m', '1h', '1D', '1W' 等
        strict_mode: 严谨模式开关，开启后只合并指定时间范围内的数据
        start_time: 严谨模式下的起始时间 (HH:mm:ss)，默认 '09:30:00'
        end_time: 严谨模式下的结束时间 (HH:mm:ss)，默认 '15:00:00'
    
    Returns:
        合并后的数据字典
    """
    ret = {}

    # 对于周线数据，使用 W-FRI（周五结束）作为 resample 参数
    resample_period = 'W-FRI' if period.upper() == '1W' else period

    agg_map = {
        'open': 'first',  # 开盘价：取第一笔
        'high': 'max',  # 最高价：取最大值
        'low': 'min',  # 最低价：取最小值
        'close': 'last',  # 收盘价：取最后一笔
        'volume': 'sum',  # 成交量：求和
        'amount': 'sum',  # 成交额：求和
    }

    for indicator, df in data_dict.items():
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # 严谨模式：只保留指定时间范围内的数据
        if strict_mode:
            df = df.between_time(start_time, end_time)

        # 1. 执行重采样
        rule = agg_map.get(indicator, 'last')
        if rule == 'sum':
            processed_df = df.resample(resample_period).sum(min_count=1)
        else:
            processed_df = df.resample(resample_period).agg(rule)

        # 2. 关键步骤：将索引改为该周期内实际存在的最后一个时间点
        # 这样 10.03(周五) 就会被重命名为该区间内最后一天有数据的日期 09.30
        if period.upper() == '1W':
            # 找到原始数据中每个周期对应的最后实际交易时间
            last_actual_time = df.index.to_series().resample(resample_period).max()
            # 过滤掉那些完全没数据的周期
            last_actual_time = last_actual_time.dropna()
            # 对齐索引
            processed_df = processed_df.loc[last_actual_time.index]
            processed_df.index = last_actual_time

        # 3. 剔除全空行
        processed_df = processed_df.dropna(how='all')

        if not period.endswith('m'):
            processed_df.index = processed_df.index.strftime('%Y-%m-%d')

        ret[indicator] = processed_df

    return ret


def simulate_period_k_data(data_dict: dict, period: str = '1D'):
    """
    通过分钟线数据计算出每一分钟的其他级别线的数据
    """
    # 结果字典
    ret = {}

    # 1. 确定分组器：按天分组
    # 使用 index.normalize() 可以将时间归一化到当天的 00:00:00，效率比 index.date 高
    # 假设所有 DataFrame 的 index 是一致的，取任意一个即可
    sample_index = data_dict['close'].index
    if period == '1D':
        grouper = sample_index.normalize()
    elif period == '1W':
        grouper = sample_index.to_period('W')

    # --- Open: 当日保持不变，取当天的第一个 open ---
    if 'open' in data_dict:
        # transform('first') 会把当天的第一个值广播到当天的每一行
        ret['open'] = data_dict['open'].groupby(grouper).transform('first')

    # --- High: 截止当前的最高价 (累计最大值) ---
    if 'high' in data_dict:
        ret['high'] = data_dict['high'].groupby(grouper).cummax()

    # --- Low: 截止当前的最低价 (累计最小值) ---
    if 'low' in data_dict:
        ret['low'] = data_dict['low'].groupby(grouper).cummin()

    # --- Close: 截止当前的收盘价 (即当前分钟的 Close) ---
    if 'close' in data_dict:
        # 不需要计算，直接拷贝引用
        ret['close'] = data_dict['close'].copy()

    # --- Volume: 截止当前的成交量 (累计求和) ---
    if 'volume' in data_dict:
        ret['volume'] = data_dict['volume'].groupby(grouper).cumsum()

    # --- Amount: 截止当前的成交额 (累计求和) ---
    if 'amount' in data_dict:
        ret['amount'] = data_dict['amount'].groupby(grouper).cumsum()

    return ret


def load_minute_k_data_from_db(
        stock_list: Optional[List[str]] = None,
        start_date_time: Optional[str] = None,
        end_date_time: Optional[str] = None,
        fields: List[str] = ['open', 'high', 'low', 'close', 'volume', 'amount'],
        source: str = 'mysql',
        mysql_reader=None,
        clickhouse_reader=None,
) -> Dict[str, pd.DataFrame]:
    """
    从数据库的 minute_k_data 表中读取分钟线数据

    Args:
        stock_list: 股票代码列表，例如 ['000001.SZ', '600519.SH']。如果为 None，则读取所有股票
        start_date_time: 开始时间，格式 'YYYY-MM-DD HH:MM:SS'，例如 '2023-01-01 09:30:00'
        end_date_time: 结束时间，格式 'YYYY-MM-DD HH:MM:SS'，例如 '2023-12-31 15:00:00'
        fields: 需要读取的字段列表，可选值：['open', 'high', 'low', 'close', 'volume', 'amount']

    Returns:
        Dict[str, pd.DataFrame]: 字典，key 为字段名，value 为 DataFrame
            - DataFrame 的 index 为时间（DatetimeIndex，格式 'YYYY-MM-DD HH:MM:SS'）
            - DataFrame 的 columns 为股票代码（例如 '000001.SZ'）
            - 数据按时间从小到大排序
    """
    result = {field: {} for field in fields}
    if source == 'mysql':
        reader = _get_mysql_reader(mysql_reader)
    elif source == 'clickhouse':
        reader = _get_clickhouse_reader(clickhouse_reader)
    else:
        raise ValueError(f"Unsupported source: {source}")

    if not stock_list:
        return {field: pd.DataFrame() for field in fields}

    try:
        for stock in stock_list:
            frame = reader.load_symbol_minutes(
                symbol=stock,
                start_datetime=start_date_time,
                end_datetime=end_date_time,
            )
            if frame.empty:
                continue
            for field in fields:
                if field in frame.columns:
                    result[field][stock] = frame[field]

        return {
            field: pd.DataFrame(series_map).sort_index() if series_map else pd.DataFrame()
            for field, series_map in result.items()
        }
    except Exception as e:
        print(f"读取数据库数据时发生错误: {e}")
        return {field: pd.DataFrame() for field in fields}


def save_minute_k_data_to_db(
        data_dict: Dict[str, pd.DataFrame],
        batch_size: int = 1000,
        bar_storage: str = 'mysql',
        mysql_storage=None,
        clickhouse_storage=None,
) -> bool:
    """
    将分钟线数据增量保存到数据库的 minute_k_data 表中

    Args:
        data_dict: 字典，key 为字段名（'open', 'high', 'low', 'close', 'volume', 'amount'），
                   value 为 DataFrame
            - DataFrame 的 index 必须是时间（DatetimeIndex 或可转换为时间的格式）
            - DataFrame 的 columns 必须是股票代码（例如 '000001.SZ'）
        batch_size: 批量插入的大小，默认 1000 条

    Returns:
        bool: 是否保存成功

    注意：
        - 使用 ON DUPLICATE KEY UPDATE 实现增量保存（如果主键已存在则更新）
        - 假设表结构：stock_code (VARCHAR), datetime (DATETIME), open, high, low, close, volume, amount
        - 主键为 (stock_code, datetime)
    """
    if not data_dict:
        print("警告：data_dict 为空，无需保存")
        return False

    storage = _get_mysql_storage(mysql_storage)
    ch_storage = _get_clickhouse_storage(clickhouse_storage) if bar_storage == 'clickhouse' else None
    connection = storage._get_connection()
    required_fields = ['open', 'high', 'low', 'close']
    optional_fields = ['volume', 'amount']

    try:
        if bar_storage not in {'mysql', 'clickhouse'}:
            raise ValueError(f"Unsupported bar_storage: {bar_storage}")
        symbol_set = set()
        for df in data_dict.values():
            if not df.empty:
                symbol_set.update(map(str, df.columns))

        if not symbol_set:
            print("警告：没有有效数据需要保存")
            return False

        for symbol in sorted(symbol_set):
            series_list = []
            for field in required_fields + optional_fields:
                df = data_dict.get(field)
                if df is None or df.empty or symbol not in df.columns:
                    continue
                series_list.append(df[symbol].rename(field))

            if not series_list:
                continue

            symbol_frame = pd.concat(series_list, axis=1).sort_index()
            for field in required_fields:
                if field not in symbol_frame.columns:
                    raise ValueError(f"{symbol} 缺少必需字段 {field}")
            if 'volume' not in symbol_frame.columns:
                symbol_frame['volume'] = 0
            if 'amount' not in symbol_frame.columns:
                symbol_frame['amount'] = 0

            symbol_frame = symbol_frame[['open', 'high', 'low', 'close', 'volume', 'amount']]
            symbol_frame = symbol_frame.dropna(subset=required_fields, how='any')
            if symbol_frame.empty:
                continue

            symbol_id = storage.upsert_symbol(symbol, conn=connection)
            rows = normalize_symbol_bar_frame(symbol_id=symbol_id, frame=symbol_frame)
            if not rows:
                continue

            for start in range(0, len(rows), max(1, batch_size)):
                batch_rows = rows[start:start + max(1, batch_size)]
                if bar_storage == 'mysql':
                    storage.upsert_bar_rows(batch_rows, conn=connection)
                else:
                    ch_storage.upsert_bar_rows(batch_rows)

        connection.commit()
        return True
    except Exception as e:
        connection.rollback()
        print(f"保存数据到数据库时发生错误: {e}")
        return False
    finally:
        storage._close_if_possible(connection)


def get_qfq_factor_from_file(filePath: str = QFQ_FACTOR_DIR) -> pd.DataFrame:
    if not os.path.exists(filePath):
        raise FileNotFoundError(f"File not found: {filePath}")
    df = pd.read_parquet(filePath)
    return df

def get_qfq_factor_from_ak(stock_code: str) -> pd.DataFrame:
    import akshare as ak
    
    # 将股票代码从xxxx.SZ或xxxx.SH转换为szxxxx或shxxxx
    if stock_code.endswith('.SZ'):
        stock_code = 'sz' + stock_code[:-3]
    elif stock_code.endswith('.SH'):
        stock_code = 'sh' + stock_code[:-3]
    else:
        raise ValueError(f"Invalid stock code: {stock_code}")
    
    return ak.stock_zh_a_daily(stock_code, adjust='qfq')
    

def calculate_qfq_data(data_dict: dict, qfq_factor_df: pd.DataFrame) -> dict:
    """计算复权数据（前复权）- 高效版本

    优化说明：
    1. 内存优化：避免创建 (N_minutes * N_stocks) 的巨大中间矩阵，改为分块处理。
    2. 速度优化：使用 numpy 广播和 fancy indexing 替代 pandas 的 reindex/map。
    3. 逻辑修正：先计算累计因子再重采样，防止因除权日无行情数据导致漏算因子。

    Args:
        data_dict (dict): 数据字典，key为open、low、close、high字符串，value为dataframe，
                          index为形如yyyy-MM-dd HH:mm:ss的分钟时间，列为股票代码
        qfq_factor_df (pd.DataFrame): 复权因子数据，index为形如yyyy-MM-dd的日期，
                                       列为股票代码，仅在除权日有值（非除权日为NaN）

    Returns:
        dict: 复权数据字典，格式与data_dict相同
    """
    if not data_dict or 'close' not in data_dict:
        return data_dict

    price_df = data_dict['close']
    price_index = price_df.index
    price_columns = price_df.columns

    # ===== 核心优化：只处理实际存在的股票列，避免无用计算 =====
    # 取交集：只处理价格数据和因子数据都有的股票
    common_columns = price_columns.intersection(qfq_factor_df.columns)
    if len(common_columns) == 0:
        # 没有交集，无法复权，直接返回原数据
        print("警告：复权因子与价格数据无交集股票，跳过复权。")
        return data_dict

    # 1. 准备结果字典
    # 先完全复制一份数据，后续直接在上面修改
    # 这样可以保持非价格字段（如 volume）不变，且不修改输入 data_dict
    result_dict = {}
    for key, df in data_dict.items():
        result_dict[key] = df.copy()

    # 2. 处理复权因子
    # 提取相关股票的因子数据并排序
    factor_df = qfq_factor_df[common_columns].sort_index()

    # 确保索引是datetime类型
    if not isinstance(factor_df.index, pd.DatetimeIndex):
        factor_df.index = pd.to_datetime(factor_df.index)

    # 3. 高效处理日期对齐
    # 使用 numpy.astype('datetime64[D]') 快速转换，比 pd.normalize() 快得多
    price_dates_v = price_index.values.astype('datetime64[D]')
    unique_dates_v = np.unique(price_dates_v)  # 获取分钟线涉及的所有交易日（已排序）

    # 4. 计算日级别的累积因子
    # 关键修复：先将因子数据扩展到所有交易日，并将 NaN 填充为 1.0（表示无变化）
    # 然后再计算累积因子，这样累积因子会在所有交易日都有正确的值
    # 假设 factor_df 存储的是当次变动因子（如 0.5），非除权日为 NaN
    
    # 先将因子数据扩展到所有交易日
    # 注意：不使用 method='ffill'，而是直接 reindex 后填充 NaN 为 1.0
    # 这样非除权日会被填充为 1.0（表示无变化），而不是上一个除权日的因子值
    factor_df_expanded = factor_df.reindex(unique_dates_v)
    
    # 将所有 NaN 填充为 1.0（表示无变化）
    # 这包括：1) 非除权日（原本就是 NaN）2) factor_df 开始前的日期
    factor_df_expanded = factor_df_expanded.fillna(1.0)
    
    # 现在计算累积因子：cumprod 会将所有日期的因子累积起来
    # 非除权日为 1.0，累积后仍为累积值；除权日为变动因子，累积后会更新
    cum_factor = factor_df_expanded.cumprod()
    
    # aligned_cum_factor 就是 cum_factor（已经对齐到所有交易日）
    aligned_cum_factor = cum_factor

    # 计算前复权比例：当日累积 / 最新日累积
    # 这样最新一天的因子为 1.0，历史价格会被按比例缩小
    final_cum = aligned_cum_factor.iloc[-1]
    qfq_ratio_daily = aligned_cum_factor.div(final_cum, axis=1)

    # 5. 构建分钟到日期的映射
    # unique_dates_v 是有序的，使用 searchsorted 快速找到每个分钟对应的日期索引
    day_indices = np.searchsorted(unique_dates_v, price_dates_v)

    # 6. 分块应用复权因子（关键内存优化）
    # 不再一次性生成所有股票的分钟级因子矩阵，而是分批处理
    chunk_size = 100  # 每次处理 100 只股票
    all_stocks = list(common_columns)
    
    # 确保 qfq_ratio_daily 列顺序与 all_stocks 一致
    qfq_ratio_daily = qfq_ratio_daily[all_stocks]
    qfq_ratio_daily_values = qfq_ratio_daily.values.astype(np.float32) # 转为 float32 节省内存

    # 遍历所有需要复权的字段
    target_fields = [f for f in ['open', 'high', 'low', 'close'] if f in result_dict]

    for i in range(0, len(all_stocks), chunk_size):
        # 当前块的股票
        chunk_stocks = all_stocks[i : i + chunk_size]
        col_indices = slice(i, i + len(chunk_stocks))

        # 获取当前块的日线因子: (N_days, N_chunk)
        chunk_ratios_daily = qfq_ratio_daily_values[:, col_indices]

        # 广播到分钟线: (N_minutes, N_chunk)
        # 使用 fancy indexing 快速映射，内存开销仅为当前块的大小
        chunk_ratios_minute = chunk_ratios_daily[day_indices]

        # 应用到结果 DataFrame
        for field in target_fields:
            # 原地修改结果 DataFrame 的对应列
            # 注意：result_dict[field] 是副本，修改安全
            # 使用 .loc[:, cols] = val 确保赋值
            result_dict[field].loc[:, chunk_stocks] = \
                result_dict[field].loc[:, chunk_stocks] * chunk_ratios_minute

    return result_dict
