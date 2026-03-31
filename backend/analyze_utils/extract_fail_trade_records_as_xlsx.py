import pandas as pd


def extract_fail_trade_records_as_xlsx(path: str):
    
    trade_record = pd.read_parquet(path=path)

    trade_record = trade_record[
        (trade_record['Status'] == 'Closed') &
        (trade_record['Return'] < 0) & 
        (
            trade_record['Column'].str.endswith('.SH') |
            trade_record['Column'].str.endswith('.SZ')
        )
    ]

    # 删除多余列
    trade_record = trade_record.drop(columns=[
        'Exit Trade Id','Entry Fees','Exit Fees','Direction','Status','Position Id'
    ])

    trade_record['Return'] = 100 * trade_record['Return']

    trade_record = trade_record.sort_values(
        by=['Column', 'Entry Timestamp'],
        ascending=[True, True]
    )

    # 修改Column为股票代码
    trade_record = trade_record.rename(columns={
        'Column': '股票代码',
        'Size': '交易股数',
        'Entry Timestamp': '买入时间',
        'Avg Entry Price': '买入均价',
        'Exit Timestamp': '卖出时间',
        'Avg Exit Price': '卖出均价',
        'PnL': '盈亏金额',
        'Return': '收益率'
    })
    
    # 将结果作为xlsx保存在同目录下
    target_path = path.replace('.parquet', '.xlsx')
    trade_record.to_excel(target_path, index=False)
    print(f"已将结果保存到 {target_path}")
    
    
if __name__ == '__main__':
    
    source_path = r'C:\Users\18917\Desktop\ProgramWorkSpace\quantitativeTradeProject\customized_backtest_tool\backend\backtest_results\Fibonacci_EMA_CROSS_Strategy_20260223_140930\trades.parquet'
    
    extract_fail_trade_records_as_xlsx(source_path)
    