from __future__ import annotations

import pandas as pd

try:
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage
except ModuleNotFoundError:
    from mysql_bar_storage import MysqlBarStorage


def rows_to_factor_map(rows: list[dict]) -> dict[str, pd.Series]:
    if not rows:
        return {}

    frame = pd.DataFrame(rows)
    if frame.empty:
        return {}

    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    factor_map: dict[str, pd.Series] = {}
    for symbol, group in frame.groupby("symbol", sort=True):
        series = pd.Series(
            group["factor"].astype(float).to_numpy(),
            index=pd.DatetimeIndex(group["trade_date"]),
            name=str(symbol),
        ).sort_index()
        factor_map[str(symbol)] = series
    return factor_map


class QfqFactorReader:
    def __init__(self, storage: MysqlBarStorage | None = None):
        self.storage = storage or MysqlBarStorage()

    def load_symbol_factor_series(
        self,
        symbol: str,
        start_date=None,
        end_date=None,
    ) -> pd.Series:
        factor_map = self.load_factor_map([symbol], start_date=start_date, end_date=end_date)
        series = factor_map.get(symbol)
        if series is not None:
            return series
        return pd.Series(dtype=float, name=symbol)

    def load_factor_map(
        self,
        symbols: list[str],
        start_date=None,
        end_date=None,
    ) -> dict[str, pd.Series]:
        if not symbols:
            return {}

        normalized_symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if symbol))
        symbol_ids = []
        for symbol in normalized_symbols:
            symbol_id = self.storage.resolve_symbol_id(symbol)
            if symbol_id:
                symbol_ids.append(symbol_id)

        rows = self.storage.load_qfq_factor_rows(
            symbol_ids,
            start_date=start_date,
            end_date=end_date,
        )
        return rows_to_factor_map(rows)
