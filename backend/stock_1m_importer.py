from __future__ import annotations

import argparse
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

try:
    from customized_backtest_tool.backend.mysql_bar_common import normalize_symbol_bar_frame
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage, init_schema
except ModuleNotFoundError:
    from mysql_bar_common import normalize_symbol_bar_frame
    from mysql_bar_storage import MysqlBarStorage, init_schema


BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BACKEND_DIR.parent
DEFAULT_STOCK_LIST_PATH = PROJECT_DIR / "stock_files" / "stock_list.csv"


def _load_data_manager():
    from customized_backtest_tool.backend import data_manager

    return data_manager


def should_skip_incremental_sync(state: dict | None, file_size: int, file_mtime: int) -> bool:
    if not state:
        return False
    # 仅上次成功同步时，才允许按文件元信息直接跳过
    if int(state.get("last_status") or 0) != 2:
        return False
    return (
        int(state.get("last_file_size") or 0) == int(file_size)
        and int(state.get("last_file_mtime") or 0) == int(file_mtime)
    )


def estimate_incremental_start_ts(
    last_trade_date: date | str | None,
    rewind_trading_days: int,
) -> int | None:
    if not last_trade_date:
        return None
    last_trade_day = pd.Timestamp(last_trade_date).date()
    safe_calendar_days = max(7, int(max(1, rewind_trading_days)) * 5)
    start_day = last_trade_day - timedelta(days=safe_calendar_days)
    return max(0, int(pd.Timestamp(start_day).timestamp()))


def compute_incremental_rows(
    rows: list[tuple],
    last_trade_date: date | None,
    rewind_trading_days: int = 3,
) -> list[tuple]:
    if not rows or last_trade_date is None:
        return rows

    historical_dates = sorted({row[0] for row in rows if row[0] <= last_trade_date})
    if not historical_dates:
        return rows

    anchor_index = max(0, len(historical_dates) - rewind_trading_days)
    anchor_date = historical_dates[anchor_index]
    return [row for row in rows if row[0] >= anchor_date]


def load_stock_catalog(stock_list_path: str | Path = DEFAULT_STOCK_LIST_PATH) -> dict[str, dict]:
    frame = pd.read_csv(stock_list_path, encoding="utf-8-sig")
    result: dict[str, dict] = {}
    for row in frame.to_dict(orient="records"):
        symbol = str(row["ts_code"]).strip().upper()
        result[symbol] = row
    return result


def iter_symbol_jobs(
    *,
    stock_list_path: str | Path = DEFAULT_STOCK_LIST_PATH,
    data_dir: str | None = None,
    symbol: str | None = None,
    start_symbol: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    manager = _load_data_manager()
    catalog = load_stock_catalog(stock_list_path)
    symbols = sorted(catalog.keys())
    if symbol:
        symbols = [symbol.strip().upper()]
    if start_symbol:
        start_symbol = start_symbol.strip().upper()
        symbols = [item for item in symbols if item >= start_symbol]
    if limit is not None:
        symbols = symbols[:limit]

    jobs: list[dict] = []
    for item in symbols:
        dat_path = manager.get_dat_file_path(item, period="1m", base_dir=data_dir)
        if not os.path.exists(dat_path):
            continue
        row = catalog.get(item, {})
        jobs.append(
            {
                "symbol": item,
                "name": row.get("name"),
                "dat_path": dat_path,
            }
        )
    return jobs


def _load_symbol_rows(symbol_id: int, dat_path: str, start_ts: int | None = None) -> list[tuple]:
    manager = _load_data_manager()
    frame = manager.read_iquant_mmap(dat_path, start_ts=start_ts)
    return normalize_symbol_bar_frame(symbol_id=symbol_id, frame=frame)


def import_symbol(
    storage: MysqlBarStorage,
    *,
    symbol: str,
    dat_path: str,
    name: str | None = None,
    incremental: bool = False,
    rewind_trading_days: int = 3,
    conn=None,
) -> int:
    file_stat = os.stat(dat_path)
    own_conn = conn is None
    connection = conn or storage._get_connection()
    try:
        symbol_id = storage.upsert_symbol(symbol, name=name, dat_path=dat_path, conn=connection)
        state = storage.load_import_state(symbol_id, conn=connection) if incremental else None

        if incremental and should_skip_incremental_sync(state, file_stat.st_size, int(file_stat.st_mtime)):
            connection.commit()
            return 0

        start_ts = None
        if incremental and state:
            start_ts = estimate_incremental_start_ts(
                last_trade_date=state.get("last_bar_trade_date"),
                rewind_trading_days=rewind_trading_days,
            )
        rows = _load_symbol_rows(symbol_id, dat_path, start_ts=start_ts)
        if incremental and state:
            rows = compute_incremental_rows(
                rows,
                last_trade_date=state.get("last_bar_trade_date"),
                rewind_trading_days=rewind_trading_days,
            )

        affected = storage.upsert_bar_rows(rows, conn=connection)
        if rows:
            last_trade_date = rows[-1][0]
            last_minute_slot = rows[-1][1]
        else:
            last_trade_date = state.get("last_bar_trade_date") if state else None
            last_minute_slot = state.get("last_bar_minute_slot") if state else None

        storage.update_import_state(
            symbol_id=symbol_id,
            dat_path=dat_path,
            file_size=file_stat.st_size,
            file_mtime=int(file_stat.st_mtime),
            last_trade_date=last_trade_date,
            last_minute_slot=last_minute_slot,
            import_mode=2 if incremental else 1,
            status=2,
            rows_affected=affected,
            error_message=None,
            conn=connection,
        )
        connection.commit()
        return affected
    except Exception as exc:
        connection.rollback()
        try:
            symbol_id = storage.resolve_symbol_id(symbol, conn=connection)
            if symbol_id:
                storage.update_import_state(
                    symbol_id=symbol_id,
                    dat_path=dat_path,
                    file_size=file_stat.st_size if os.path.exists(dat_path) else 0,
                    file_mtime=int(file_stat.st_mtime) if os.path.exists(dat_path) else 0,
                    last_trade_date=None,
                    last_minute_slot=None,
                    import_mode=2 if incremental else 1,
                    status=3,
                    rows_affected=0,
                    error_message=str(exc)[:500],
                    conn=connection,
                )
                connection.commit()
        except Exception:
            connection.rollback()
        raise
    finally:
        if own_conn:
            storage._close_if_possible(connection)


def run_import(
    *,
    incremental: bool,
    data_dir: str | None,
    stock_list_path: str | Path,
    symbol: str | None,
    start_symbol: str | None,
    limit: int | None,
    rewind_trading_days: int,
) -> dict:
    storage = MysqlBarStorage()
    jobs = iter_symbol_jobs(
        stock_list_path=stock_list_path,
        data_dir=data_dir,
        symbol=symbol,
        start_symbol=start_symbol,
        limit=limit,
    )

    job_id = storage.create_job(2 if incremental else 1, len(jobs))
    success = 0
    failed = 0
    inserted_rows = 0
    connection = storage._get_connection()
    try:
        for job in jobs:
            try:
                inserted_rows += import_symbol(
                    storage,
                    symbol=job["symbol"],
                    dat_path=job["dat_path"],
                    name=job["name"],
                    incremental=incremental,
                    rewind_trading_days=rewind_trading_days,
                    conn=connection,
                )
                success += 1
            except Exception:
                failed += 1
    finally:
        storage._close_if_possible(connection)

    storage.update_job(
        job_id,
        status=2 if failed == 0 else 3,
        success_symbol_count=success,
        failed_symbol_count=failed,
        inserted_rows=inserted_rows,
        updated_rows=0,
        finished_at=pd.Timestamp.now().to_pydatetime(),
    )
    return {
        "job_id": job_id,
        "symbol_count": len(jobs),
        "success_symbol_count": success,
        "failed_symbol_count": failed,
        "inserted_rows": inserted_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MySQL 分钟线导入工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-schema")

    full_parser = subparsers.add_parser("full-import")
    full_parser.add_argument("--data-dir", default=None)
    full_parser.add_argument("--stock-list-path", default=str(DEFAULT_STOCK_LIST_PATH))
    full_parser.add_argument("--symbol", default=None)
    full_parser.add_argument("--start-symbol", default=None)
    full_parser.add_argument("--limit", type=int, default=None)

    incremental_parser = subparsers.add_parser("incremental-sync")
    incremental_parser.add_argument("--data-dir", default=None)
    incremental_parser.add_argument("--stock-list-path", default=str(DEFAULT_STOCK_LIST_PATH))
    incremental_parser.add_argument("--symbol", default=None)
    incremental_parser.add_argument("--start-symbol", default=None)
    incremental_parser.add_argument("--limit", type=int, default=None)
    incremental_parser.add_argument("--rewind-trading-days", type=int, default=3)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-schema":
        init_schema()
        print("MySQL schema 初始化完成")
        return 0

    if args.command == "full-import":
        result = run_import(
            incremental=False,
            data_dir=args.data_dir,
            stock_list_path=args.stock_list_path,
            symbol=args.symbol,
            start_symbol=args.start_symbol,
            limit=args.limit,
            rewind_trading_days=3,
        )
        print(result)
        return 0

    if args.command == "incremental-sync":
        result = run_import(
            incremental=True,
            data_dir=args.data_dir,
            stock_list_path=args.stock_list_path,
            symbol=args.symbol,
            start_symbol=args.start_symbol,
            limit=args.limit,
            rewind_trading_days=args.rewind_trading_days,
        )
        print(result)
        return 0

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
