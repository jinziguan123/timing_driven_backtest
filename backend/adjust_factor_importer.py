from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow.parquet as pq

try:
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage
except ModuleNotFoundError:
    from mysql_bar_storage import MysqlBarStorage


DEFAULT_QFQ_FACTOR_PATH = Path(
    os.environ.get("QFQ_FACTOR_DIR", r"C:\Users\18917\Desktop\merged_adjust_factors.parquet")
)


def _load_parquet_symbol_columns(file_path: str | Path) -> list[str]:
    schema = pq.read_schema(file_path)
    names = list(schema.names)
    metadata = schema.metadata or {}
    pandas_meta_raw = metadata.get(b"pandas")
    if not pandas_meta_raw:
        return names

    pandas_meta = json.loads(pandas_meta_raw.decode("utf-8"))
    index_columns: set[str] = set()
    for item in pandas_meta.get("index_columns", []):
        if isinstance(item, str):
            index_columns.add(item)
        elif isinstance(item, dict):
            field_name = item.get("field_name")
            if field_name:
                index_columns.add(str(field_name))

    return [name for name in names if name not in index_columns]


def iter_factor_column_chunks(
    file_path: str | Path,
    chunk_size: int = 500,
) -> Iterator[tuple[list[str], pd.DataFrame]]:
    symbols = _load_parquet_symbol_columns(file_path)
    if not symbols:
        return

    effective_chunk_size = max(1, int(chunk_size))
    for start in range(0, len(symbols), effective_chunk_size):
        chunk_symbols = symbols[start:start + effective_chunk_size]
        yield chunk_symbols, pd.read_parquet(file_path, columns=chunk_symbols)


def build_factor_rows(
    frame: pd.DataFrame,
    symbols: list[str],
    symbol_map: dict[str, int],
    source_file_mtime: int,
) -> list[tuple]:
    rows: list[tuple] = []
    for symbol in symbols:
        symbol_id = symbol_map.get(symbol)
        if not symbol_id or symbol not in frame.columns:
            continue
        series = frame[symbol].dropna()
        if series.empty:
            continue
        for trade_date, factor in series.items():
            rows.append(
                (
                    int(symbol_id),
                    pd.Timestamp(trade_date).date(),
                    float(factor),
                    int(source_file_mtime),
                )
            )
    return rows


def run_import(
    file_path: str | Path = DEFAULT_QFQ_FACTOR_PATH,
    chunk_size: int = 500,
    storage: MysqlBarStorage | None = None,
) -> dict:
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"前复权因子文件不存在: {file_path}")

    storage = storage or MysqlBarStorage()
    source_file_mtime = int(file_path.stat().st_mtime)
    total_rows = 0
    total_symbols = 0

    for symbols, frame in iter_factor_column_chunks(file_path, chunk_size=chunk_size):
        symbol_map: dict[str, int] = {}
        connection = storage._get_connection()
        try:
            for symbol in symbols:
                symbol_map[symbol] = storage.upsert_symbol(symbol, conn=connection)
            rows = build_factor_rows(frame, symbols, symbol_map, source_file_mtime=source_file_mtime)
            affected = storage.upsert_qfq_factor_rows(rows, conn=connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            storage._close_if_possible(connection)

        total_symbols += len(symbols)
        total_rows += affected

    return {
        "file_path": str(file_path),
        "symbol_count": total_symbols,
        "row_count": total_rows,
        "source_file_mtime": source_file_mtime,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="前复权因子导入工具")
    parser.add_argument(
        "command",
        nargs="?",
        default="full-import",
        choices=["full-import"],
        help="当前仅支持 full-import",
    )
    parser.add_argument("--file-path", default=str(DEFAULT_QFQ_FACTOR_PATH))
    parser.add_argument("--chunk-size", type=int, default=500)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_import(file_path=args.file_path, chunk_size=args.chunk_size)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
