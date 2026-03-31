from __future__ import annotations

from datetime import datetime

import pandas as pd

try:
    from customized_backtest_tool.backend.mysql_bar_common import (
        minute_slot_to_time_text,
        restore_amount_from_k,
    )
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage
except ModuleNotFoundError:
    from mysql_bar_common import minute_slot_to_time_text, restore_amount_from_k
    from mysql_bar_storage import MysqlBarStorage


def _combine_trade_date_and_slot(trade_date, minute_slot: int) -> pd.Timestamp:
    trade_date_text = str(trade_date)
    return pd.Timestamp(f"{trade_date_text} {minute_slot_to_time_text(int(minute_slot))}")


def rows_to_frame(rows) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "amount"])

    frame = pd.DataFrame(rows).copy()
    frame["datetime"] = [
        _combine_trade_date_and_slot(trade_date, minute_slot)
        for trade_date, minute_slot in zip(frame["trade_date"], frame["minute_slot"])
    ]
    frame["amount"] = frame["amount_k"].map(restore_amount_from_k)
    result = frame.set_index("datetime")[["open", "high", "low", "close", "volume", "amount"]]
    result.index = pd.DatetimeIndex(result.index)
    return result.sort_index()


class MysqlBarReader:
    def __init__(self, storage: MysqlBarStorage | None = None):
        self.storage = storage or MysqlBarStorage()

    def _fetch_all(self, sql: str, params: tuple) -> list[dict]:
        connection = self.storage._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return list(cursor.fetchall() or [])
        finally:
            self.storage._close_if_possible(connection)

    def _build_pool_join(self, owner_key: str | None, pool_name: str | None) -> tuple[str, tuple]:
        if not owner_key or not pool_name:
            return "", ()
        return (
            """
            JOIN stock_pool_symbol p ON p.symbol_id = s.symbol_id
            JOIN stock_pool pool ON pool.pool_id = p.pool_id
            """,
            (owner_key, pool_name),
        )

    def load_symbol_minutes(
        self,
        symbol: str,
        start_datetime: str | None = None,
        end_datetime: str | None = None,
    ) -> pd.DataFrame:
        conditions = ["s.symbol = %s"]
        params: list = [symbol]
        if start_datetime:
            start_ts = pd.Timestamp(start_datetime)
            conditions.append("(b.trade_date > %s OR (b.trade_date = %s AND b.minute_slot >= %s))")
            params.extend([start_ts.date(), start_ts.date(), start_ts.hour * 60 + start_ts.minute])
        if end_datetime:
            end_ts = pd.Timestamp(end_datetime)
            conditions.append("(b.trade_date < %s OR (b.trade_date = %s AND b.minute_slot <= %s))")
            params.extend([end_ts.date(), end_ts.date(), end_ts.hour * 60 + end_ts.minute])

        rows = self._fetch_all(
            f"""
            SELECT b.trade_date, b.minute_slot, b.open, b.high, b.low, b.close, b.volume, b.amount_k
            FROM stock_bar_1m b
            JOIN stock_symbol s ON s.symbol_id = b.symbol_id
            WHERE {' AND '.join(conditions)}
            ORDER BY b.trade_date ASC, b.minute_slot ASC
            """,
            tuple(params),
        )
        return rows_to_frame(rows)

    def load_trade_day(
        self,
        trade_date: str,
        owner_key: str | None = None,
        pool_name: str | None = None,
    ) -> pd.DataFrame:
        pool_join, pool_params = self._build_pool_join(owner_key, pool_name)
        conditions = ["b.trade_date = %s"]
        params: list = [trade_date]
        if pool_join:
            conditions.extend(["pool.owner_key = %s", "pool.pool_name = %s"])
            params.extend(pool_params)
        rows = self._fetch_all(
            f"""
            SELECT s.symbol, b.trade_date, b.minute_slot, b.open, b.high, b.low, b.close, b.volume, b.amount_k
            FROM stock_bar_1m b
            JOIN stock_symbol s ON s.symbol_id = b.symbol_id
            {pool_join}
            WHERE {' AND '.join(conditions)}
            ORDER BY b.minute_slot ASC, s.symbol ASC
            """,
            tuple(params),
        )
        if not rows:
            return pd.DataFrame(
                columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
            )
        frame = pd.DataFrame(rows)
        frame["datetime"] = [
            _combine_trade_date_and_slot(trade_date_value, minute_slot)
            for trade_date_value, minute_slot in zip(frame["trade_date"], frame["minute_slot"])
        ]
        frame["amount"] = frame["amount_k"].map(restore_amount_from_k)
        return frame[["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]]

    def load_minute_snapshot(
        self,
        trade_date: str,
        minute_slot: int,
        owner_key: str | None = None,
        pool_name: str | None = None,
    ) -> pd.DataFrame:
        pool_join, pool_params = self._build_pool_join(owner_key, pool_name)
        conditions = ["b.trade_date = %s", "b.minute_slot = %s"]
        params: list = [trade_date, minute_slot]
        if pool_join:
            conditions.extend(["pool.owner_key = %s", "pool.pool_name = %s"])
            params.extend(pool_params)
        rows = self._fetch_all(
            f"""
            SELECT s.symbol, b.trade_date, b.minute_slot, b.open, b.high, b.low, b.close, b.volume, b.amount_k
            FROM stock_bar_1m b
            JOIN stock_symbol s ON s.symbol_id = b.symbol_id
            {pool_join}
            WHERE {' AND '.join(conditions)}
            ORDER BY s.symbol ASC
            """,
            tuple(params),
        )
        if not rows:
            return pd.DataFrame(columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"])
        frame = pd.DataFrame(rows)
        frame["datetime"] = [
            _combine_trade_date_and_slot(trade_date_value, slot)
            for trade_date_value, slot in zip(frame["trade_date"], frame["minute_slot"])
        ]
        frame["amount"] = frame["amount_k"].map(restore_amount_from_k)
        return frame[["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]]
