from __future__ import annotations

import argparse
import concurrent.futures
import multiprocessing
import os
import time
from pathlib import Path

import pandas as pd

try:
    from customized_backtest_tool.backend.clickhouse_bar_storage import (
        ClickHouseBarStorage,
        init_schema as init_clickhouse_schema,
    )
    from customized_backtest_tool.backend.mysql_bar_storage import (
        MysqlBarStorage,
        init_schema as init_mysql_schema,
    )
    from customized_backtest_tool.backend.stock_1m_importer import (
        DEFAULT_STOCK_LIST_PATH,
        compute_incremental_rows,
        estimate_incremental_start_ts,
        iter_symbol_jobs,
        should_skip_incremental_sync,
        _load_symbol_rows,
    )
except ModuleNotFoundError:
    from clickhouse_bar_storage import ClickHouseBarStorage, init_schema as init_clickhouse_schema
    from mysql_bar_storage import MysqlBarStorage, init_schema as init_mysql_schema
    from stock_1m_importer import (
        DEFAULT_STOCK_LIST_PATH,
        compute_incremental_rows,
        estimate_incremental_start_ts,
        iter_symbol_jobs,
        should_skip_incremental_sync,
        _load_symbol_rows,
    )


def init_schema() -> None:
    init_mysql_schema()
    init_clickhouse_schema()


def build_progress_snapshot(
    *,
    started_at: float,
    done_symbols: int,
    total_symbols: int,
    success_symbols: int,
    failed_symbols: int,
    inserted_rows: int,
    now: float | None = None,
) -> dict:
    current = float(now if now is not None else time.time())
    elapsed = max(current - float(started_at), 1e-6)
    return {
        "done_symbols": int(done_symbols),
        "total_symbols": int(total_symbols),
        "success_symbols": int(success_symbols),
        "failed_symbols": int(failed_symbols),
        "inserted_rows": int(inserted_rows),
        "elapsed_seconds": elapsed,
        "symbols_per_second": float(done_symbols) / elapsed,
        "rows_per_second": float(inserted_rows) / elapsed,
    }


def print_progress(prefix: str, snapshot: dict) -> None:
    print(
        f"[{prefix}] {snapshot['done_symbols']}/{snapshot['total_symbols']} "
        f"success={snapshot['success_symbols']} failed={snapshot['failed_symbols']} "
        f"rows={snapshot['inserted_rows']} "
        f"symbol/s={snapshot['symbols_per_second']:.2f} "
        f"rows/s={snapshot['rows_per_second']:.2f} "
        f"elapsed={snapshot['elapsed_seconds']:.1f}s"
    )


def import_symbol(
    mysql_storage: MysqlBarStorage,
    ch_storage: ClickHouseBarStorage,
    *,
    symbol: str,
    dat_path: str,
    name: str | None = None,
    incremental: bool = False,
    rewind_trading_days: int = 3,
    mysql_conn=None,
    ch_client=None,
) -> int:
    file_stat = os.stat(dat_path)
    own_mysql_conn = mysql_conn is None
    own_ch_client = ch_client is None
    connection = mysql_conn or mysql_storage._get_connection()
    client = ch_client or ch_storage._get_client()
    try:
        symbol_id = mysql_storage.upsert_symbol(symbol, name=name, dat_path=dat_path, conn=connection)
        state = mysql_storage.load_import_state(symbol_id, conn=connection) if incremental else None

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

        sync_version = time.time_ns()
        affected = ch_storage.upsert_bar_rows(rows, version=sync_version, client=client)
        if rows:
            last_trade_date = rows[-1][0]
            last_minute_slot = rows[-1][1]
        else:
            last_trade_date = state.get("last_bar_trade_date") if state else None
            last_minute_slot = state.get("last_bar_minute_slot") if state else None

        mysql_storage.update_import_state(
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
            symbol_id = mysql_storage.resolve_symbol_id(symbol, conn=connection)
            if symbol_id:
                mysql_storage.update_import_state(
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
        if own_mysql_conn:
            mysql_storage._close_if_possible(connection)
        if own_ch_client:
            ch_storage._close_if_possible(client)


def _import_symbol_worker(payload: dict) -> dict:
    mysql_storage = MysqlBarStorage()
    ch_storage = ClickHouseBarStorage()
    try:
        affected = import_symbol(
            mysql_storage,
            ch_storage,
            symbol=str(payload["symbol"]),
            dat_path=str(payload["dat_path"]),
            name=payload.get("name"),
            incremental=bool(payload.get("incremental", False)),
            rewind_trading_days=int(payload.get("rewind_trading_days", 3)),
        )
        return {
            "symbol": str(payload["symbol"]),
            "ok": True,
            "affected": int(affected),
            "error": None,
        }
    except Exception as exc:
        return {
            "symbol": str(payload["symbol"]),
            "ok": False,
            "affected": 0,
            "error": str(exc)[:500],
        }


def _resolve_workers(workers: int | None) -> int:
    if workers is not None:
        return max(1, int(workers))
    cpu_count = os.cpu_count() or 1
    return max(1, min(8, cpu_count))


def run_import(
    *,
    incremental: bool,
    data_dir: str | None,
    stock_list_path: str | Path,
    symbol: str | None,
    start_symbol: str | None,
    limit: int | None,
    rewind_trading_days: int,
    workers: int | None = None,
    log_every: int = 50,
) -> dict:
    mysql_storage = MysqlBarStorage()
    ch_storage = ClickHouseBarStorage()
    jobs = iter_symbol_jobs(
        stock_list_path=stock_list_path,
        data_dir=data_dir,
        symbol=symbol,
        start_symbol=start_symbol,
        limit=limit,
    )

    job_id = mysql_storage.create_job(2 if incremental else 1, len(jobs))
    if not jobs:
        mysql_storage.update_job(
            job_id,
            status=2,
            success_symbol_count=0,
            failed_symbol_count=0,
            inserted_rows=0,
            updated_rows=0,
            finished_at=pd.Timestamp.now().to_pydatetime(),
            note="没有可导入的 DAT 文件",
        )
        return {
            "job_id": job_id,
            "symbol_count": 0,
            "success_symbol_count": 0,
            "failed_symbol_count": 0,
            "inserted_rows": 0,
            "workers": 0,
        }

    effective_workers = _resolve_workers(workers)
    log_every = max(1, int(log_every))
    success = 0
    failed = 0
    inserted_rows = 0
    started_at = time.time()
    total = len(jobs)
    done = 0
    failed_details: list[tuple[str, str]] = []

    if effective_workers == 1:
        mysql_conn = mysql_storage._get_connection()
        ch_client = ch_storage._get_client()
        try:
            for job in jobs:
                try:
                    inserted_rows += import_symbol(
                        mysql_storage,
                        ch_storage,
                        symbol=job["symbol"],
                        dat_path=job["dat_path"],
                        name=job["name"],
                        incremental=incremental,
                        rewind_trading_days=rewind_trading_days,
                        mysql_conn=mysql_conn,
                        ch_client=ch_client,
                    )
                    success += 1
                except Exception as exc:
                    failed += 1
                    failed_details.append((str(job["symbol"]), str(exc)[:200]))
                done += 1
                if done % log_every == 0 or done == total:
                    snapshot = build_progress_snapshot(
                        started_at=started_at,
                        done_symbols=done,
                        total_symbols=total,
                        success_symbols=success,
                        failed_symbols=failed,
                        inserted_rows=inserted_rows,
                    )
                    print_progress("progress", snapshot)
        finally:
            mysql_storage._close_if_possible(mysql_conn)
            ch_storage._close_if_possible(ch_client)
    else:
        ctx = multiprocessing.get_context("spawn")
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=effective_workers,
            mp_context=ctx,
        ) as executor:
            future_to_symbol = {
                executor.submit(
                    _import_symbol_worker,
                    {
                        "symbol": job["symbol"],
                        "dat_path": job["dat_path"],
                        "name": job["name"],
                        "incremental": incremental,
                        "rewind_trading_days": rewind_trading_days,
                    },
                ): str(job["symbol"])
                for job in jobs
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol_text = future_to_symbol[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "symbol": symbol_text,
                        "ok": False,
                        "affected": 0,
                        "error": str(exc)[:500],
                    }

                if result.get("ok"):
                    success += 1
                    inserted_rows += int(result.get("affected") or 0)
                else:
                    failed += 1
                    failed_details.append((symbol_text, str(result.get("error") or "unknown error")[:200]))

                done += 1
                if done % log_every == 0 or done == total:
                    snapshot = build_progress_snapshot(
                        started_at=started_at,
                        done_symbols=done,
                        total_symbols=total,
                        success_symbols=success,
                        failed_symbols=failed,
                        inserted_rows=inserted_rows,
                    )
                    print_progress("progress", snapshot)

    mysql_storage.update_job(
        job_id,
        status=2 if failed == 0 else 3,
        success_symbol_count=success,
        failed_symbol_count=failed,
        inserted_rows=inserted_rows,
        updated_rows=0,
        finished_at=pd.Timestamp.now().to_pydatetime(),
    )
    if failed_details:
        sample = "; ".join([f"{symbol}:{message}" for symbol, message in failed_details[:5]])
        print(f"[warning] failed_samples={sample}")
    final_snapshot = build_progress_snapshot(
        started_at=started_at,
        done_symbols=done,
        total_symbols=total,
        success_symbols=success,
        failed_symbols=failed,
        inserted_rows=inserted_rows,
    )
    print_progress("final", final_snapshot)
    return {
        "job_id": job_id,
        "symbol_count": len(jobs),
        "success_symbol_count": success,
        "failed_symbol_count": failed,
        "inserted_rows": inserted_rows,
        "workers": effective_workers,
        "symbols_per_second": final_snapshot["symbols_per_second"],
        "rows_per_second": final_snapshot["rows_per_second"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ClickHouse 分钟线导入工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-schema")

    full_parser = subparsers.add_parser("full-import")
    full_parser.add_argument("--data-dir", default=None)
    full_parser.add_argument("--stock-list-path", default=str(DEFAULT_STOCK_LIST_PATH))
    full_parser.add_argument("--symbol", default=None)
    full_parser.add_argument("--start-symbol", default=None)
    full_parser.add_argument("--limit", type=int, default=None)
    full_parser.add_argument("--workers", type=int, default=None)
    full_parser.add_argument("--log-every", type=int, default=50)

    incremental_parser = subparsers.add_parser("incremental-sync")
    incremental_parser.add_argument("--data-dir", default=None)
    incremental_parser.add_argument("--stock-list-path", default=str(DEFAULT_STOCK_LIST_PATH))
    incremental_parser.add_argument("--symbol", default=None)
    incremental_parser.add_argument("--start-symbol", default=None)
    incremental_parser.add_argument("--limit", type=int, default=None)
    incremental_parser.add_argument("--rewind-trading-days", type=int, default=3)
    incremental_parser.add_argument("--workers", type=int, default=None)
    incremental_parser.add_argument("--log-every", type=int, default=50)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-schema":
        init_schema()
        print("MySQL 元数据表 + ClickHouse 分钟线表初始化完成")
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
            workers=args.workers,
            log_every=args.log_every,
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
            workers=args.workers,
            log_every=args.log_every,
        )
        print(result)
        return 0

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
