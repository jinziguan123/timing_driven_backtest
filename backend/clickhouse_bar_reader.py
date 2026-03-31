from __future__ import annotations

import pandas as pd

try:
    from customized_backtest_tool.backend.clickhouse_bar_storage import ClickHouseBarStorage
    from customized_backtest_tool.backend.mysql_bar_common import minute_slot_to_time_text, restore_amount_from_k
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage
except ModuleNotFoundError:
    from clickhouse_bar_storage import ClickHouseBarStorage
    from mysql_bar_common import minute_slot_to_time_text, restore_amount_from_k
    from mysql_bar_storage import MysqlBarStorage


def _combine_trade_date_and_slot(trade_date, minute_slot: int) -> pd.Timestamp:
    trade_date_text = str(trade_date)
    return pd.Timestamp(f"{trade_date_text} {minute_slot_to_time_text(int(minute_slot))}")


def _rows_to_frame(rows, columns) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "amount"])

    frame = pd.DataFrame(rows, columns=columns)
    frame["datetime"] = [
        _combine_trade_date_and_slot(trade_date, minute_slot)
        for trade_date, minute_slot in zip(frame["trade_date"], frame["minute_slot"])
    ]
    frame["amount"] = frame["amount_k"].map(restore_amount_from_k)
    result = frame.set_index("datetime")[["open", "high", "low", "close", "volume", "amount"]]
    result.index = pd.DatetimeIndex(result.index)
    return result.sort_index()


class ClickHouseBarReader:
    def __init__(
        self,
        bar_storage: ClickHouseBarStorage | None = None,
        mysql_storage: MysqlBarStorage | None = None,
    ):
        self.bar_storage = bar_storage or ClickHouseBarStorage()
        self.mysql_storage = mysql_storage or MysqlBarStorage()

    def _query(self, sql: str, params: dict | None = None, client=None) -> tuple[list, list]:
        own_client = client is None
        ch_client = client or self.bar_storage._get_client()
        try:
            query_result = ch_client.query(sql, parameters=params or {})
            rows = list(getattr(query_result, "result_rows", []) or [])
            columns = list(getattr(query_result, "column_names", []) or [])
            return rows, columns
        finally:
            if own_client:
                self.bar_storage._close_if_possible(ch_client)

    def _resolve_symbol_id(self, symbol: str) -> int | None:
        return self.mysql_storage.resolve_symbol_id(symbol)

    def _load_pool_symbol_ids(self, owner_key: str | None, pool_name: str | None) -> list[int]:
        if not owner_key or not pool_name:
            return []
        pool = self.mysql_storage.get_pool(owner_key, pool_name)
        if not pool:
            return []
        rows = self.mysql_storage.list_pool_symbols(int(pool["pool_id"]))
        return [int(row["symbol_id"]) for row in rows]

    def _load_symbol_map(self, symbol_ids: list[int] | None = None) -> dict[int, str]:
        connection = self.mysql_storage._get_connection()
        try:
            with connection.cursor() as cursor:
                if symbol_ids:
                    placeholders = ", ".join(["%s"] * len(symbol_ids))
                    cursor.execute(
                        f"SELECT symbol_id, symbol FROM stock_symbol WHERE symbol_id IN ({placeholders})",
                        tuple(symbol_ids),
                    )
                else:
                    cursor.execute("SELECT symbol_id, symbol FROM stock_symbol")
                rows = cursor.fetchall() or []
            return {int(row["symbol_id"]): str(row["symbol"]) for row in rows}
        finally:
            self.mysql_storage._close_if_possible(connection)

    def load_symbol_minutes(
        self,
        symbol: str,
        start_datetime: str | None = None,
        end_datetime: str | None = None,
    ) -> pd.DataFrame:
        symbol_id = self._resolve_symbol_id(symbol)
        if symbol_id is None:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume", "amount"])

        conditions = ["symbol_id = {symbol_id:UInt32}"]
        params: dict = {"symbol_id": int(symbol_id)}
        if start_datetime:
            start_ts = pd.Timestamp(start_datetime)
            conditions.append(
                "(trade_date > {start_date:Date} OR (trade_date = {start_date:Date} AND minute_slot >= {start_slot:UInt16}))"
            )
            params["start_date"] = start_ts.date()
            params["start_slot"] = int(start_ts.hour * 60 + start_ts.minute)
        if end_datetime:
            end_ts = pd.Timestamp(end_datetime)
            conditions.append(
                "(trade_date < {end_date:Date} OR (trade_date = {end_date:Date} AND minute_slot <= {end_slot:UInt16}))"
            )
            params["end_date"] = end_ts.date()
            params["end_slot"] = int(end_ts.hour * 60 + end_ts.minute)

        rows, columns = self._query(
            f"""
            SELECT
                trade_date,
                minute_slot,
                argMax(open, version) AS open,
                argMax(high, version) AS high,
                argMax(low, version) AS low,
                argMax(close, version) AS close,
                argMax(volume, version) AS volume,
                argMax(amount_k, version) AS amount_k
            FROM stock_bar_1m
            WHERE {' AND '.join(conditions)}
            GROUP BY trade_date, minute_slot
            ORDER BY trade_date ASC, minute_slot ASC
            """,
            params,
        )
        return _rows_to_frame(rows, columns)

    def load_trade_day(
        self,
        trade_date: str,
        owner_key: str | None = None,
        pool_name: str | None = None,
    ) -> pd.DataFrame:
        symbol_ids = self._load_pool_symbol_ids(owner_key, pool_name)
        if owner_key and pool_name and not symbol_ids:
            return pd.DataFrame(
                columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
            )

        conditions = ["trade_date = {trade_date:Date}"]
        params: dict = {"trade_date": pd.Timestamp(trade_date).date()}
        if symbol_ids:
            conditions.append("symbol_id IN {symbol_ids:Array(UInt32)}")
            params["symbol_ids"] = [int(item) for item in symbol_ids]

        rows, columns = self._query(
            f"""
            SELECT
                symbol_id,
                trade_date,
                minute_slot,
                argMax(open, version) AS open,
                argMax(high, version) AS high,
                argMax(low, version) AS low,
                argMax(close, version) AS close,
                argMax(volume, version) AS volume,
                argMax(amount_k, version) AS amount_k
            FROM stock_bar_1m
            WHERE {' AND '.join(conditions)}
            GROUP BY symbol_id, trade_date, minute_slot
            ORDER BY minute_slot ASC, symbol_id ASC
            """,
            params,
        )
        if not rows:
            return pd.DataFrame(
                columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
            )

        frame = pd.DataFrame(rows, columns=columns)
        symbol_map = self._load_symbol_map(sorted(set(int(item) for item in frame["symbol_id"])))
        frame["symbol"] = frame["symbol_id"].map(symbol_map).fillna("")
        frame["datetime"] = [
            _combine_trade_date_and_slot(trade_date_value, slot)
            for trade_date_value, slot in zip(frame["trade_date"], frame["minute_slot"])
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
        symbol_ids = self._load_pool_symbol_ids(owner_key, pool_name)
        if owner_key and pool_name and not symbol_ids:
            return pd.DataFrame(
                columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
            )

        conditions = ["trade_date = {trade_date:Date}", "minute_slot = {minute_slot:UInt16}"]
        params: dict = {
            "trade_date": pd.Timestamp(trade_date).date(),
            "minute_slot": int(minute_slot),
        }
        if symbol_ids:
            conditions.append("symbol_id IN {symbol_ids:Array(UInt32)}")
            params["symbol_ids"] = [int(item) for item in symbol_ids]

        rows, columns = self._query(
            f"""
            SELECT
                symbol_id,
                trade_date,
                minute_slot,
                argMax(open, version) AS open,
                argMax(high, version) AS high,
                argMax(low, version) AS low,
                argMax(close, version) AS close,
                argMax(volume, version) AS volume,
                argMax(amount_k, version) AS amount_k
            FROM stock_bar_1m
            WHERE {' AND '.join(conditions)}
            GROUP BY symbol_id, trade_date, minute_slot
            ORDER BY symbol_id ASC
            """,
            params,
        )
        if not rows:
            return pd.DataFrame(
                columns=["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]
            )

        frame = pd.DataFrame(rows, columns=columns)
        symbol_map = self._load_symbol_map(sorted(set(int(item) for item in frame["symbol_id"])))
        frame["symbol"] = frame["symbol_id"].map(symbol_map).fillna("")
        frame["datetime"] = [
            _combine_trade_date_and_slot(trade_date_value, slot)
            for trade_date_value, slot in zip(frame["trade_date"], frame["minute_slot"])
        ]
        frame["amount"] = frame["amount_k"].map(restore_amount_from_k)
        return frame[["symbol", "datetime", "open", "high", "low", "close", "volume", "amount"]]
