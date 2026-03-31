from data_manager import load_data_generator, prepare_vbt_data
from engine import BacktestEngine
from strategies import (
    BIAS_MACD_JINZUAN, 
    BIAS_EXPMA_Strategy, 
    EXPMA_Signal1_Strategy, 
    Grid_Strategy, 
    Test_Strategy,
    Week_EMA_Strategy,
    Fibonacci_EMA_Strategy,
    Fibonacci_EMA_BIAS_Strategy,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy,
    Fibonacci_EMA_BIAS_Lock_Main_Up_Wave_Strategy,
    Fibonacci_EMA_TOP_ENTRANCE_Strategy,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V2,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V3,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V4,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V5,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V6,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V7,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V8,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V11,
    Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12,
    Fibonacci_EMA_CROSS_Strategy
)
from result_saver import ResultSaver
import pandas as pd
import gc
import math
import traceback

# 强制所有浮点数显示为 2 位小数
pd.set_option('display.float_format', '{:.2f}'.format)

ANALYZE_FLAG = False
SAVE_FLAG = True
BATCH_SIZE = 500 # 每次处理 1000 只股票
BAR_SOURCE = 'clickhouse'
ADJUST_MODE = 'qfq'
ADJUST_FACTOR_SOURCE = 'mysql'

def main():
    # 1. 基础配置
    # 请确保这些股票的 .DAT 文件存在于 LOCAL_DATA_DIR 中
    stock_list = pd.read_csv('../stock_files/stock_list.csv')['ts_code'].tolist()
    # 示例：仅取前2500个进行测试，实际运行时可以去掉切片
    # stock_list = stock_list[:100] 
    # stock_list = ['000001.SZ'] 
    
    start_time = "2023-01-01 09:30:00"
    end_time = "2026-12-01 15:00:00"
    
    # 策略参数
    init_cash = 1000000
    
    # 准备聚合容器
    aggregated_results = {
        'stats_dict': {},
        'equity_df_list': [],
        'orders_df_list': [],
        'trades_df_list': [],
        'positions_df_list': [],
        'symbols_list': []
    }
    
    total_stocks = len(stock_list)
    num_batches = math.ceil(total_stocks / BATCH_SIZE)
    
    # 初始化策略实例 (注意：如果策略有内部状态需要在batch间重置，请在循环内初始化)
    # 大多数vbt策略是无状态的或者状态仅在generate_signals内有效
    # strategy = BIAS_MACD_JINZUAN()
    strategy = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V12()
    # strategy = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V9()
    # strategy = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_Strategy_V10()
    # strategy = Test_Strategy()
    
    print(f"开始分批回测：共 {total_stocks} 只股票，分为 {num_batches} 批次。")
    
    for i in range(num_batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, total_stocks)
        current_batch_stocks = stock_list[start_idx:end_idx]
        
        print(f"\n>>> 处理批次 {i+1}/{num_batches}: 股票数量 {len(current_batch_stocks)}")
        
        # 2. 数据加载 (Batch)
        print(">>> 正在加载数据...")
        provider = load_data_generator(stock_list=current_batch_stocks, 
                                       start_date_time=start_time, 
                                       end_date_time=end_time, 
                                       period='1m',
                                       source=BAR_SOURCE,
                                       adjust=ADJUST_MODE)
        data_dict = prepare_vbt_data(provider, qfq_flag=False)
        
        if not data_dict:
            print(f"警告: 批次 {i+1} 未能加载任何数据，跳过。")
            continue

        # 3. 初始化引擎 (Batch)
        engine = BacktestEngine(
            data_dict, 
            init_cash=init_cash,
            fees=0.0002,
            slippage=0.0002,
            max_positions=5,
            position_size_pct=0.19,
            random_seed=100
        )
        
        # 4. 运行策略 (Batch)
        try:
            engine.run(strategy)
            
            # 5. 提取并缓存结果
            batch_results = engine.get_batch_results()
            if batch_results:
                aggregated_results['stats_dict'].update(batch_results['stats_dict'])
                aggregated_results['equity_df_list'].append(batch_results['equity_df'])
                aggregated_results['orders_df_list'].append(batch_results['orders_df'])
                aggregated_results['trades_df_list'].append(batch_results['trades_df'])
                aggregated_results['positions_df_list'].append(batch_results['positions_df'])
                aggregated_results['symbols_list'].extend(batch_results['symbols'])
                print(f">>> 批次 {i+1} 完成，已缓存有效数据。")
            else:
                print(f">>> 批次 {i+1} 运行完成，但无有效交易产生。")
                
        except Exception as e:
            print(f"错误: 批次 {i+1} 运行出错: {e}")
            traceback.print_exc()
        
        # 6. 内存清理
        del engine
        del data_dict
        gc.collect()
        
    # 7. 最终保存
    if SAVE_FLAG and aggregated_results['symbols_list']:
        print("\n>>> 所有批次完成，正在聚合保存...")
        saver = ResultSaver()
        saver.save_aggregated_result(
            aggregated_data=aggregated_results,
            strategy_name=strategy.name,
            init_cash=init_cash,
            additional_info={
                'bar_source': BAR_SOURCE,
                'adjust_mode': ADJUST_MODE,
                'adjust_factor_source': ADJUST_FACTOR_SOURCE,
            }
        )
    else:
        print("\n>>> 未产生任何有效回测结果，未保存。")

if __name__ == "__main__":
    main()
