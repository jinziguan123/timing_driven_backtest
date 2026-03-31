# 自定义回测工具

基于vectorbt的回测框架，支持EXPMA信号1策略，包含完整的仓位管理、止盈止损、结果持久化和可视化功能。

## 功能特性

1. **EXPMA信号1策略**
   - 实现EXPMA(60)和EXPMA(120)指标
   - KDJ信号线计算
   - 多条件选股逻辑

2. **组合仓位管理**
   - 最多同时持仓20只股票
   - 每只股票按当前总资产的5%买入（复利）
   - 10%止盈、7%止损
   - 随机买入顺序（可设置随机种子）

3. **回测引擎**
   - 基于vectorbt的高性能回测
   - 支持动态资金分配
   - T+1交易规则
   - 支持将回测结果持久化并通过API提供给前端展示

4. **结果持久化**
   - 保存回测结果到JSON和Parquet格式
   - 包含统计信息、净值曲线、交易记录、持仓记录等
   - 额外保存日线、分钟线数据，便于前端交互（点击日K跳转分钟线、查看当日成交）

5. **可视化展示**
   - K线蜡烛图（日线）与买卖点标注（红色向上箭头表示买入，绿色向下箭头表示卖出）
   - 绝对持仓股数展示（显示实际持有的股票数量）
   - 累计盈亏曲线
   - 策略净值曲线
   - 参考掘金量化的可视化风格

6. **性能优化**
   - 使用Numba加速核心计算
   - 向量化计算
   - 内存优化（float32、mmap等）

## 文件结构

```
customized_backtest_tool/
├── strategy.py          # 策略类定义（EXPMA_Signal1_Strategy）
├── position_manager.py  # 组合仓位管理器
├── engine.py           # 回测引擎
├── data_manager.py     # 数据加载和管理
├── analysis.py         # 可视化分析
├── result_saver.py     # 结果持久化
└── main.py            # 主入口文件
```

## 使用方法

### 1. 基本使用

```python
from data_manager import load_data_generator, prepare_vbt_data
from engine import BacktestEngine
from strategy import EXPMA_Signal1_Strategy
from result_saver import ResultSaver

# 1. 配置回测参数
stock_list = ['000001.SZ', '000002.SZ', '600519.SH']
start_time = "2023-01-01 09:30:00"
end_time = "2023-06-01 15:00:00"

# 2. 加载数据
provider = load_data_generator(
    stock_list=stock_list, 
    start_date_time=start_time, 
    end_date_time=end_time, 
    period='1m',
    source='clickhouse',
    adjust='qfq',
)
data_dict = prepare_vbt_data(provider, qfq_flag=False)

# 3. 初始化引擎
engine = BacktestEngine(
    data_dict, 
    init_cash=1000000,      # 100万初始资金
    fees=0.0002,            # 万二佣金
    slippage=0.0002,        # 滑点
    max_positions=20,       # 最多20只持仓
    tp_pct=0.10,           # 10%止盈
    sl_pct=0.07,           # 7%止损
    position_size_pct=0.05, # 每只股票5%资金
    random_seed=42          # 随机种子（可复现）
)

# 4. 运行策略
strategy = EXPMA_Signal1_Strategy()
engine.run(strategy)

# 5. 保存结果
saver = ResultSaver()
result_id = engine.save_result(strategy.name, saver=saver)

# 6. 查看统计和图表
engine.analyze()
engine.analyze(specific_stock='000001.SZ')  # 查看单只股票详情
```

### 2. 加载历史回测结果

```python
from result_saver import ResultSaver

saver = ResultSaver()
result = saver.load_backtest_result('EXPMA_Signal1_20231201_120000')

# 查看统计信息
print(result['stats'])

# 查看净值曲线
equity = result['equity']
print(equity)

# 查看交易记录
orders = result['orders']
print(orders)
```

### 3. 列出所有回测结果

```python
from result_saver import ResultSaver

saver = ResultSaver()
results_df = saver.list_results()
print(results_df)
```

## 策略说明

### EXPMA信号1策略

选股条件（需同时满足）：

1. **EXPMA条件1**: EXPMA(60) > EXPMA(120)
2. **EXPMA条件2**: EXPMA(120) 必须上涨（EXPMA(120) > REF(EXPMA(120), 1)）
3. **KDJ信号线条件**: 
   - 信号线1昨 < 3
   - 信号线1昨 < 信号线1前
   - 信号线1 > 信号线1昨
4. **KDJ D值条件**: D < 25

其中：
- 信号线1 = 3*K - 2*D
- K = SMA(RSV, 3, 1)
- D = SMA(K, 3, 1)
- RSV = ((CLOSE - LLV(LOW,9)) / (HHV(HIGH,9) - LLV(LOW,9))) * 100

### 交易规则

- **买入**: 满足选股条件时买入
- **卖出**: 
  - 达到10%止盈点
  - 达到7%止损点
- **资金分配**: 每只股票按当前总资产的5%买入（复利）
- **持仓限制**: 最多同时持仓20只股票
- **买入顺序**: 随机打乱顺序买入（可设置随机种子）

## 依赖要求

```bash
pip install -r customized_backtest_tool/backend/requirements.txt
```

## 注意事项

1. **数据格式**: 数据需要符合iQuant的数据格式（mmap读取）
2. **内存优化**: 大数据量回测时注意内存使用，已优化为float32和mmap
3. **随机种子**: 设置random_seed可以保证回测结果可复现
4. **T+1规则**: 当日买入的股票当日不能卖出

## 结果文件说明

回测结果保存在 `backtest_results/` 目录下，每个回测结果包含：

- `stats.json`: 统计信息
- `equity.parquet`: 净值曲线
- `orders.parquet`: 交易记录
- `trades.parquet`: 交易成交明细（含PnL、持仓状态）
- `positions.parquet`: 持仓记录
- `entries.parquet`: 买入信号
- `exits.parquet`: 卖出信号
- `size_matrix.parquet`: 买入数量矩阵
- `metadata.json`: 元数据
- `daily_ohlcv.parquet`: 日线聚合数据（open/high/low/close/volume）
- `minute/{symbol}.parquet`: 各股票分钟线数据，便于前端按日筛选

## API 与前端联动

已内置 FastAPI 服务，便于前端（React/Vue 等现代框架或纯 HTML）直接拉取历史回测结果并做交互展示。

### 安装依赖

```bash
pip install "fastapi>=0.110" "uvicorn[standard]" pydantic
```

### 启动 API

```bash
cd customized_backtest_tool
uvicorn api_server:app --reload --port 8000
```

### 主要接口

- `GET /api/results`：列出已保存的回测结果列表（含策略名、时间、末值、收益率）。
- `GET /api/results/{result_id}/summary`：返回该次回测的元数据、统计指标、净值序列（默认尾部 400 点）。
- `GET /api/results/{result_id}/daily?symbol=600519.SH`：获取日线OHLCV数据，可选按股票过滤。
- `GET /api/results/{result_id}/minute?symbol=600519.SH&date=2024-01-05`：获取指定日期的分钟线数据，供点击日K后跳转展示。
- `GET /api/results/{result_id}/orders?symbol=600519.SH&date=2024-01-05`：获取某日的成交记录，方便在分钟线下方列出交易明细。
- `GET /api/results/{result_id}/trades?symbol=600519.SH`：获取交易闭环/持仓明细。

前端交互范式示例：
1. 页面加载时调用 `/api/results` 渲染历史回测列表。

## MySQL 分钟线入库

新增脚本基于 MySQL 存储 `1m` 分钟线与股票池，推荐按下面顺序执行：

### 1. 初始化依赖

```bash
source /Users/jinziguan/.virtualenvs/Python_Calculation/bin/activate
pip install -r customized_backtest_tool/backend/requirements.txt
```

如果迁移到其他电脑，不需要复用这条虚拟环境路径，只需要保证用任意 Python 3.10+ 环境安装同一份 `requirements.txt`。

### 2. 配置环境变量

```bash
export MYSQL_HOST=172.30.26.12
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=你的密码
export MYSQL_DATABASE=quant_data
export MYSQL_CHARSET=utf8mb4

export IQUANT_LOCAL_DATA_DIR="/你的/datadir"
```

### 3. 初始化库表

```bash
python3 -m customized_backtest_tool.backend.stock_1m_importer init-schema
```

### 4. 全量导入分钟线

```bash
python3 -m customized_backtest_tool.backend.stock_1m_importer full-import \
  --stock-list-path customized_backtest_tool/stock_files/stock_list.csv
```

只导入单只股票：

```bash
python3 -m customized_backtest_tool.backend.stock_1m_importer full-import \
  --symbol 600519.SH
```

### 5. 增量同步最新分钟线

默认会回看最近 3 个交易日，并使用 upsert 修正已有 bar：

```bash
python3 -m customized_backtest_tool.backend.stock_1m_importer incremental-sync
```

自定义回看窗口：

```bash
python3 -m customized_backtest_tool.backend.stock_1m_importer incremental-sync \
  --rewind-trading-days 5
```

### 6. 股票池管理

创建或覆盖股票池：

```bash
python3 -m customized_backtest_tool.backend.stock_pool_manager create-or-replace-pool \
  --owner-key demo \
  --pool-name core_a \
  --symbols "600519.SH,000001.SZ,000002.SZ"
```

追加股票：

```bash
python3 -m customized_backtest_tool.backend.stock_pool_manager append-symbols \
  --owner-key demo \
  --pool-name core_a \
  --symbols "300750.SZ"
```

查看股票池：

```bash
python3 -m customized_backtest_tool.backend.stock_pool_manager show-pool \
  --owner-key demo \
  --pool-name core_a
```

### 7. 从 MySQL 读取分钟线

```python
from data_manager import load_stock_minutes

df = load_stock_minutes(
    "600519.SH",
    start_date_time="2026-03-17 09:31:00",
    end_date_time="2026-03-17 15:00:00",
    source="mysql",
)
```

默认 `source='dat'`，设置为 `mysql` 或 `clickhouse` 后会走数据库 reader。
如果希望直接返回前复权价格，可以额外传 `adjust='qfq'`。

## ClickHouse 分钟线存储（推荐）

该方案采用混合架构：

1. MySQL 保存元数据和管理表（`stock_symbol`、`stock_pool`、`stock_bar_1m_import_state`、`stock_bar_import_job`）。
2. ClickHouse 保存大体量 `1m` K 线事实表（`stock_bar_1m`）。

### 1. 配置环境变量

```bash
export MYSQL_HOST=172.30.26.12
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=你的密码
export MYSQL_DATABASE=quant_data
export MYSQL_CHARSET=utf8mb4

export CLICKHOUSE_HOST=127.0.0.1
export CLICKHOUSE_PORT=8123
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=
export CLICKHOUSE_DATABASE=quant_data
export CLICKHOUSE_SECURE=0

export IQUANT_LOCAL_DATA_DIR="/你的/datadir"
```

### 2. 初始化库表

```bash
python3 -m customized_backtest_tool.backend.clickhouse_1m_importer init-schema
```

### 3. 全量导入分钟线（写入 ClickHouse）

```bash
python3 -m customized_backtest_tool.backend.clickhouse_1m_importer full-import \
  --stock-list-path customized_backtest_tool/stock_files/stock_list.csv \
  --workers 6 \
  --log-every 100
```

### 4. 增量同步最新分钟线（写入 ClickHouse）

```bash
python3 -m customized_backtest_tool.backend.clickhouse_1m_importer incremental-sync \
  --rewind-trading-days 3 \
  --workers 6 \
  --log-every 50
```

导入过程中会输出吞吐日志，例如：

```text
[progress] 200/3000 success=198 failed=2 rows=432000 symbol/s=8.31 rows/s=17952.42 elapsed=24.1s
[final] 3000/3000 success=2992 failed=8 rows=6523410 symbol/s=9.02 rows/s=19612.85 elapsed=332.6s
```

### 5. 从 ClickHouse 读取分钟线

```python
from data_manager import load_stock_minutes

df = load_stock_minutes(
    "600519.SH",
    start_date_time="2026-03-17 09:31:00",
    end_date_time="2026-03-17 15:00:00",
    source="clickhouse",
    adjust="qfq",
)
```

也可以批量读取宽表格式：

```python
from data_manager import load_minute_k_data_from_db

data = load_minute_k_data_from_db(
    stock_list=["600519.SH", "000001.SZ"],
    start_date_time="2026-03-17 09:31:00",
    end_date_time="2026-03-17 15:00:00",
    source="clickhouse",
)
```

### 6. 导入前复权因子到 MySQL

前复权因子仍以本地 `merged_adjust_factors.parquet` 为上游真源，但运行时查询会统一从 MySQL 读取：

```bash
python3 -m customized_backtest_tool.backend.adjust_factor_importer full-import \
  --file-path "$QFQ_FACTOR_DIR" \
  --chunk-size 500
```

导入策略：

1. 按股票列分块读取 `parquet`，避免整张宽表一次性展开。
2. 只把非空事件日写入 MySQL 表 `stock_qfq_factor`。
3. 回测时按当前批次股票预取因子，并在单股分钟线层面完成前复权。

### 7. 当前推荐回测配置

当前推荐的组合是：

1. `source='clickhouse'`
2. `adjust='qfq'`
3. `prepare_vbt_data(provider, qfq_flag=False)`

这样可以避免先拼完整宽表再统一前复权，显著减少额外内存占用。

2. 选择某个回测后调用 `/summary` 和 `/daily` 绘制净值、日K。
3. 用户点击日K的某个日期时，携带 symbol + date 调 `/minute` 拉分钟线，叠加 `/orders` 展示当日买卖点/成交表格。
4. 如需盈亏统计，调用 `/trades` 或直接使用 summary 的 stats 字段。

## 性能优化

1. **Numba加速**: 核心计算逻辑使用Numba JIT编译
2. **向量化计算**: 使用pandas和numpy的向量化操作
3. **内存优化**: 
   - 使用float32减少内存占用
   - 使用mmap进行内存映射读取
   - 及时释放中间变量

## 可视化说明

可视化图表包含4个子图，**所有数据均以日线展示**：

1. **股价走势与买卖点**: 
   - 使用K线蜡烛图展示日线数据（红涨绿跌，符合中国市场习惯）
   - 标注买入点（红色向上箭头，位于K线下方）
   - 标注卖出点（绿色向下箭头，位于K线上方）
   - 买卖点按日期聚合显示，鼠标悬停可查看当日交易总量和手续费

2. **持仓状态**: 
   - 显示**绝对持仓股数**（实际持有的股票数量）
   - 非交易量的相对变化
   - 数据重采样为日线，展示每日收盘时的持仓状态

3. **累计盈亏**: 
   - 显示已实现盈亏的累计曲线
   - 数据重采样为日线展示

4. **策略净值曲线**: 
   - 显示策略净值变化
   - 数据重采样为日线展示

## 许可证

MIT License
