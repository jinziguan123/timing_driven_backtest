from __future__ import annotations

import argparse
import re

try:
    from customized_backtest_tool.backend.mysql_bar_common import normalize_symbol
    from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage
except ModuleNotFoundError:
    from mysql_bar_common import normalize_symbol
    from mysql_bar_storage import MysqlBarStorage


def normalize_symbols_text(text: str) -> list[str]:
    raw_parts = [item.strip() for item in re.split(r"[\s,;]+", text) if item.strip()]
    normalized: list[str] = []
    seen: set[str] = set()
    for part in raw_parts:
        symbol = normalize_symbol(part)
        if symbol not in seen:
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def _resolve_symbol_ids(storage: MysqlBarStorage, symbols: list[str], conn=None) -> list[int]:
    symbol_ids: list[int] = []
    for symbol in symbols:
        symbol_id = storage.upsert_symbol(symbol, conn=conn)
        symbol_ids.append(symbol_id)
    return symbol_ids


def create_or_replace_pool(
    storage: MysqlBarStorage,
    owner_key: str,
    pool_name: str,
    symbols: list[str],
    description: str = "",
) -> int:
    connection = storage._get_connection()
    try:
        pool_id = storage.get_or_create_pool(owner_key, pool_name, description=description, conn=connection)
        symbol_ids = _resolve_symbol_ids(storage, symbols, conn=connection)
        storage.replace_pool_symbols(pool_id, symbol_ids, conn=connection)
        connection.commit()
        return pool_id
    except Exception:
        connection.rollback()
        raise
    finally:
        storage._close_if_possible(connection)


def append_symbols(storage: MysqlBarStorage, owner_key: str, pool_name: str, symbols: list[str]) -> int:
    connection = storage._get_connection()
    try:
        pool_id = storage.get_or_create_pool(owner_key, pool_name, conn=connection)
        symbol_ids = _resolve_symbol_ids(storage, symbols, conn=connection)
        storage.append_pool_symbols(pool_id, symbol_ids, conn=connection)
        connection.commit()
        return pool_id
    except Exception:
        connection.rollback()
        raise
    finally:
        storage._close_if_possible(connection)


def remove_symbols(storage: MysqlBarStorage, owner_key: str, pool_name: str, symbols: list[str]) -> int:
    connection = storage._get_connection()
    try:
        pool = storage.get_pool(owner_key, pool_name, conn=connection)
        if not pool:
            raise ValueError(f"股票池不存在: {owner_key}/{pool_name}")
        symbol_ids = [
            storage.resolve_symbol_id(symbol, conn=connection)
            for symbol in symbols
        ]
        storage.remove_pool_symbols(pool["pool_id"], [symbol_id for symbol_id in symbol_ids if symbol_id], conn=connection)
        connection.commit()
        return int(pool["pool_id"])
    except Exception:
        connection.rollback()
        raise
    finally:
        storage._close_if_possible(connection)


def show_pool(storage: MysqlBarStorage, owner_key: str, pool_name: str) -> dict:
    pool = storage.get_pool(owner_key, pool_name)
    if not pool:
        raise ValueError(f"股票池不存在: {owner_key}/{pool_name}")
    symbols = storage.list_pool_symbols(int(pool["pool_id"]))
    return {
        "pool_id": int(pool["pool_id"]),
        "owner_key": pool["owner_key"],
        "pool_name": pool["pool_name"],
        "description": pool["description"],
        "symbols": [item["symbol"] for item in symbols],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="股票池管理工具")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create_parser = subparsers.add_parser("create-or-replace-pool")
    create_parser.add_argument("--owner-key", required=True)
    create_parser.add_argument("--pool-name", required=True)
    create_parser.add_argument("--symbols", required=True)
    create_parser.add_argument("--description", default="")

    append_parser = subparsers.add_parser("append-symbols")
    append_parser.add_argument("--owner-key", required=True)
    append_parser.add_argument("--pool-name", required=True)
    append_parser.add_argument("--symbols", required=True)

    remove_parser = subparsers.add_parser("remove-symbols")
    remove_parser.add_argument("--owner-key", required=True)
    remove_parser.add_argument("--pool-name", required=True)
    remove_parser.add_argument("--symbols", required=True)

    show_parser = subparsers.add_parser("show-pool")
    show_parser.add_argument("--owner-key", required=True)
    show_parser.add_argument("--pool-name", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    storage = MysqlBarStorage()

    if args.command == "create-or-replace-pool":
        pool_id = create_or_replace_pool(
            storage,
            owner_key=args.owner_key,
            pool_name=args.pool_name,
            symbols=normalize_symbols_text(args.symbols),
            description=args.description,
        )
        print(f"已创建或覆盖股票池: {pool_id}")
        return 0

    if args.command == "append-symbols":
        pool_id = append_symbols(
            storage,
            owner_key=args.owner_key,
            pool_name=args.pool_name,
            symbols=normalize_symbols_text(args.symbols),
        )
        print(f"已追加股票池成分: {pool_id}")
        return 0

    if args.command == "remove-symbols":
        pool_id = remove_symbols(
            storage,
            owner_key=args.owner_key,
            pool_name=args.pool_name,
            symbols=normalize_symbols_text(args.symbols),
        )
        print(f"已移除股票池成分: {pool_id}")
        return 0

    if args.command == "show-pool":
        payload = show_pool(storage, owner_key=args.owner_key, pool_name=args.pool_name)
        print(payload)
        return 0

    raise ValueError(f"未知命令: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
