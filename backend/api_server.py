"""
FastAPI 服务：提供回测结果查询接口，便于前端交互展示。
启动示例：
    uvicorn api_server:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any
import pandas as pd
import numpy as np
import math
import re

from result_saver import ResultSaver


# ======================== 输入校验工具 ========================

# result_id 仅允许字母、数字、下划线、短横线、点号，防止路径遍历
_RESULT_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_\-\.]+$')
# 日期格式校验 YYYY-MM-DD
_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
# 股票代码格式校验（如 600519.SH, 000001.SZ）
_SYMBOL_PATTERN = re.compile(r'^\d{6}\.[A-Z]{2,3}$')


def validate_result_id(result_id: str) -> str:
    """校验 result_id，防止路径遍历攻击（如 ../../etc/passwd）"""
    if not result_id or not _RESULT_ID_PATTERN.match(result_id):
        raise HTTPException(
            status_code=400,
            detail=f"无效的 result_id: '{result_id}'，仅允许字母、数字、下划线、短横线和点号"
        )
    # 额外检查：不允许包含 .. 防止路径遍历
    if '..' in result_id:
        raise HTTPException(status_code=400, detail="result_id 不允许包含 '..'")
    return result_id


def validate_date(date_str: Optional[str], param_name: str = "date") -> Optional[str]:
    """校验日期格式 YYYY-MM-DD"""
    if date_str is None:
        return None
    if not _DATE_PATTERN.match(date_str):
        raise HTTPException(
            status_code=400,
            detail=f"无效的日期格式 '{param_name}': '{date_str}'，应为 YYYY-MM-DD"
        )
    # 验证日期是否合法
    try:
        pd.to_datetime(date_str)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"无效的日期值 '{param_name}': '{date_str}'"
        )
    return date_str


def validate_symbol(symbol: Optional[str]) -> Optional[str]:
    """校验股票代码格式"""
    if symbol is None:
        return None
    if not _SYMBOL_PATTERN.match(symbol):
        raise HTTPException(
            status_code=400,
            detail=f"无效的股票代码: '{symbol}'，应为类似 600519.SH 的格式"
        )
    return symbol

app = FastAPI(title="Backtest Result API", version="0.2.0")
saver = ResultSaver()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def df_to_records(df: pd.DataFrame):
    if df is None or df.empty:
        return []
    df = df.copy()
    df = df.astype(object)
    df.replace([np.inf, -np.inf], None, inplace=True)
    df.replace({np.nan: None}, inplace=True)
    df = df.map(lambda x: None if (isinstance(x, float) and math.isnan(x)) else x)
    if isinstance(df.index, pd.DatetimeIndex):
        df.reset_index(inplace=True)
        df.rename(columns={"index": "datetime"}, inplace=True)
        df["datetime"] = df["datetime"].astype(str)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in col if x is not None]) for col in df.columns.tolist()]
    df.replace({np.nan: None}, inplace=True)
    return df.to_dict(orient="records")


def sanitize_value(v):
    if v is None:
        return None
    if isinstance(v, (float, np.floating, int, np.integer)):
        v = float(v)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    return v


def sanitize_obj(obj: Any):
    """递归清洗对象中的 NaN/Inf 为 None，避免 JSON dumps 报错"""
    if isinstance(obj, dict):
        return {k: sanitize_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        t = [sanitize_obj(v) for v in obj]
        return type(obj)(t) if not isinstance(obj, tuple) else t
    if isinstance(obj, (float, np.floating, int, np.integer)):
        return sanitize_value(obj)
    return obj


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/results")
def list_results():
    df = saver.list_results()
    return df_to_records(df)


@app.get("/api/results/{result_id}/summary")
def get_summary(
    result_id: str,
    symbol: Optional[str] = Query(None, description="股票代码，如 600519.SH"),
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    # 注意：不再返回 equity 字段，如需权益曲线请调用 /equity 接口
    data = saver.load_backtest_result(result_id)

    # stats：支持按股票选择
    stats_json = data.get("stats", {})
    if isinstance(stats_json, dict):
        stats_json = saver.select_stats_for_symbol(stats_json, symbol)

    # 如果有时间过滤，重新计算统计数据
    if start_date or end_date:
        filtered_stats = saver.get_filtered_stats(result_id, symbol, start_date, end_date)
        if filtered_stats:
            stats_json = filtered_stats

    payload = {
        "metadata": data.get("metadata", {}),
        "stats": stats_json,
    }
    return sanitize_obj(payload)


@app.get("/api/results/{result_id}/equity")
def get_equity(
    result_id: str,
    symbol: Optional[str] = Query(None, description="若指定则返回个股每日权益，否则返回组合总权益"),
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    """
    获取每日收盘资产曲线
    - 不传 symbol: 返回组合总权益 (total_equity)
    - 传 symbol: 返回该股票的每日权益 (value)
    - 支持 start_date 和 end_date 时间区间过滤
    """
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    if symbol:
        # 获取个股权益
        df = saver.load_stock_daily_equity(result_id, symbol)
        if df.empty:
            return []
        # 重命名列以便前端统一处理
        df = df.rename(columns={symbol: "value"})
    else:
        # 获取组合总权益
        df = saver.load_total_daily_equity(result_id)
        if df.empty:
            return []
        # 重命名列以便前端统一处理 (可选，或者前端区分处理)
        df = df.rename(columns={"total_equity": "value"})

    # 应用时间过滤
    df = saver.filter_df_by_date_range(df, start_date, end_date)

    return df_to_records(df)


@app.get("/api/results/{result_id}/daily")
def get_daily(
    result_id: str,
    symbol: Optional[str] = Query(None, description="股票代码"),
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    if not symbol:
        raise HTTPException(status_code=400, detail="symbol 不能为空")
    df = saver.load_daily_ohlcv(result_id, symbol)

    # 应用时间过滤
    df = saver.filter_df_by_date_range(df, start_date, end_date)

    return df_to_records(df)


@app.get("/api/results/{result_id}/minute")
def get_minute(
    result_id: str,
    symbol: str = Query(..., description="股票代码，如 600519.SH"),
    date: Optional[str] = Query(None, description="日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    date = validate_date(date, "date")

    # 1) 动态加载分钟 OHLCV（不再依赖落盘行情）
    df = saver.load_minute_ohlcv(result_id, symbol, date)
    if df.empty:
        raise HTTPException(status_code=404, detail="未找到对应的分钟线数据")

    # 2) 叠加持仓
    pos_series = saver.load_positions(result_id, symbol, date)
    if not pos_series.empty:
        pos_aligned = pos_series.reindex(df.index, method='ffill').fillna(0)
        df["position"] = pos_aligned
    else:
        df["position"] = 0.0

    # 3) 当日累计平仓盈亏（基于 trades.parquet）
    trades_df = saver.load_trades(result_id, symbol)
    if not trades_df.empty and date:
        date_idx = pd.to_datetime(date)
        day_trades = trades_df[
            pd.to_datetime(trades_df["Exit Timestamp"]).dt.normalize() == date_idx.normalize()
        ].copy()

        if not day_trades.empty:
            pnl_series = pd.Series(
                data=day_trades["PnL"].values,
                index=pd.to_datetime(day_trades["Exit Timestamp"])
            )
            pnl_series = pnl_series.groupby(level=0).sum()
            pnl_aligned = pnl_series.reindex(df.index, fill_value=0)
            df["cumulative_pnl"] = pnl_aligned.cumsum()
        else:
            df["cumulative_pnl"] = 0.0
    else:
        df["cumulative_pnl"] = 0.0

    return df_to_records(df)


@app.get("/api/results/{result_id}/orders")
def get_orders(
    result_id: str,
    symbol: Optional[str] = Query(None, description="股票代码"),
    date: Optional[str] = Query(None, description="日期，格式 YYYY-MM-DD"),
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    date = validate_date(date, "date")
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    df = saver.load_orders(result_id, symbol, date, start_date, end_date)
    return df_to_records(df)


@app.get("/api/results/{result_id}/trades")
def get_trades(
    result_id: str,
    symbol: Optional[str] = None,
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    df = saver.load_trades(result_id, symbol, start_date, end_date)
    return df_to_records(df)


@app.get("/api/results/{result_id}/positions")
def get_positions(
    result_id: str,
    symbol: Optional[str] = None,
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    data = saver.load_backtest_result(result_id)
    positions = data.get("positions")
    if positions is None or (isinstance(positions, pd.DataFrame) and positions.empty):
        return []

    if isinstance(positions, pd.DataFrame) and not isinstance(positions.index, pd.DatetimeIndex):
        try:
            positions.index = pd.to_datetime(positions.index)
        except Exception:
            pass

    # 应用时间过滤
    if isinstance(positions, pd.DataFrame):
        positions = saver.filter_df_by_date_range(positions, start_date, end_date)

    if symbol and isinstance(positions, pd.DataFrame):
        if symbol in positions.columns:
            pos_series = positions[symbol]
            result = []
            for idx, val in pos_series.items():
                result.append({
                    "datetime": str(idx) if not isinstance(idx, pd.Timestamp) else idx.isoformat(),
                    "position": float(val) if pd.notna(val) else 0.0
                })
            return result
        return []

    return df_to_records(positions if isinstance(positions, pd.DataFrame) else pd.DataFrame())


@app.get("/api/results/{result_id}/pnl")
def get_pnl(
    result_id: str,
    symbol: Optional[str] = None,
    start_date: Optional[str] = Query(None, description="开始日期，格式 YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="结束日期，格式 YYYY-MM-DD"),
):
    result_id = validate_result_id(result_id)
    symbol = validate_symbol(symbol)
    start_date = validate_date(start_date, "start_date")
    end_date = validate_date(end_date, "end_date")

    result_dir = saver.save_dir / result_id
    trades_path = result_dir / "trades.parquet"
    if not trades_path.exists():
        return []
    df = pd.read_parquet(trades_path)
    if symbol:
        df = df[df["Column"] == symbol]

    if df.empty:
        return []

    closed_trades = df[df.get("Status", pd.Series(["Closed"] * len(df))) == "Closed"]
    if closed_trades.empty:
        return []

    # 应用时间过滤（基于 Exit Timestamp）
    if start_date or end_date:
        closed_trades = closed_trades.copy()
        exit_dates = pd.to_datetime(closed_trades["Exit Timestamp"]).dt.normalize()
        if start_date:
            start_dt = pd.to_datetime(start_date)
            closed_trades = closed_trades[exit_dates >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            closed_trades = closed_trades[exit_dates <= end_dt]
    
    if closed_trades.empty:
        return []

    closed_trades = closed_trades.sort_values("Exit Timestamp")
    closed_trades["cumulative_pnl"] = closed_trades["PnL"].cumsum()

    result = []
    for _, row in closed_trades.iterrows():
        result.append({
            "datetime": str(row.get("Exit Timestamp", "")),
            "pnl": float(row.get("PnL", 0)) if pd.notna(row.get("PnL")) else 0.0,
            "cumulative_pnl": float(row.get("cumulative_pnl", 0)) if pd.notna(row.get("cumulative_pnl")) else 0.0
        })
    return result


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
