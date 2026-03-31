from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

try:
    from customized_backtest_tool.backend.mysql_bar_common import decode_market_code, normalize_symbol
    from customized_backtest_tool.backend.mysql_bar_schema import build_schema_sql
except ModuleNotFoundError:
    from mysql_bar_common import decode_market_code, normalize_symbol
    from mysql_bar_schema import build_schema_sql


@dataclass(frozen=True)
class MysqlConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"


def build_mysql_config_from_env() -> MysqlConfig:
    return MysqlConfig(
        host=os.environ.get("MYSQL_HOST", "172.30.26.12"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", "Jinziguan123"),
        database=os.environ.get("MYSQL_DATABASE", "quant_data"),
        charset=os.environ.get("MYSQL_CHARSET", "utf8mb4"),
    )


def _import_pymysql():
    try:
        import pymysql
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少 PyMySQL 依赖，请先安装 requirements.txt") from exc
    return pymysql


def connect_mysql(config: MysqlConfig | None = None, include_database: bool = True):
    pymysql = _import_pymysql()
    config = config or build_mysql_config_from_env()
    kwargs = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "password": config.password,
        "charset": config.charset,
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": False,
    }
    if include_database:
        kwargs["database"] = config.database
    return pymysql.connect(**kwargs)


def ensure_database(config: MysqlConfig | None = None) -> None:
    config = config or build_mysql_config_from_env()
    connection = connect_mysql(config, include_database=False)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{config.database}` CHARACTER SET {config.charset}"
            )
        connection.commit()
    finally:
        connection.close()


def init_schema(config: MysqlConfig | None = None) -> None:
    config = config or build_mysql_config_from_env()
    ensure_database(config)
    connection = connect_mysql(config, include_database=True)
    try:
        sql_script = build_schema_sql()
        with connection.cursor() as cursor:
            for statement in [item.strip() for item in sql_script.split(";") if item.strip()]:
                cursor.execute(statement)
        connection.commit()
    finally:
        connection.close()


class MysqlBarStorage:
    def __init__(self, connection_factory: Callable[[], object] | None = None):
        self._connection_factory = connection_factory or (lambda: connect_mysql())

    def _get_connection(self):
        return self._connection_factory()

    @staticmethod
    def _close_if_possible(connection) -> None:
        close_method = getattr(connection, "close", None)
        if callable(close_method):
            close_method()

    def upsert_symbol(
        self,
        symbol: str,
        name: str | None = None,
        dat_path: str | None = None,
        is_active: int = 1,
        conn=None,
    ) -> int:
        own_conn = conn is None
        connection = conn or self._get_connection()
        market, code, normalized = decode_market_code(symbol)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stock_symbol (symbol, code, market, name, dat_path, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        symbol_id = LAST_INSERT_ID(symbol_id),
                        code = VALUES(code),
                        market = VALUES(market),
                        name = COALESCE(VALUES(name), name),
                        dat_path = COALESCE(VALUES(dat_path), dat_path),
                        is_active = VALUES(is_active)
                    """,
                    (normalized, code, market, name, dat_path, is_active),
                )
                symbol_id = int(cursor.lastrowid or 0)
            if own_conn:
                connection.commit()
            if symbol_id <= 0:
                raise RuntimeError(f"无法获取 symbol_id: {normalized}")
            return symbol_id
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def resolve_symbol_id(self, symbol: str, conn=None) -> int | None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        normalized = normalize_symbol(symbol)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT symbol_id FROM stock_symbol WHERE symbol = %s", (normalized,))
                row = cursor.fetchone()
            return None if not row else int(row["symbol_id"])
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def load_import_state(self, symbol_id: int, conn=None) -> dict | None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM stock_bar_1m_import_state WHERE symbol_id = %s",
                    (symbol_id,),
                )
                return cursor.fetchone()
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def update_import_state(
        self,
        *,
        symbol_id: int,
        dat_path: str,
        file_size: int,
        file_mtime: int,
        last_trade_date,
        last_minute_slot: int | None,
        import_mode: int,
        status: int,
        rows_affected: int,
        error_message: str | None,
        conn=None,
    ) -> None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stock_bar_1m_import_state (
                        symbol_id,
                        dat_path,
                        last_file_size,
                        last_file_mtime,
                        last_bar_trade_date,
                        last_bar_minute_slot,
                        last_import_mode,
                        last_status,
                        last_rows_affected,
                        last_error,
                        last_started_at,
                        last_finished_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        dat_path = VALUES(dat_path),
                        last_file_size = VALUES(last_file_size),
                        last_file_mtime = VALUES(last_file_mtime),
                        last_bar_trade_date = VALUES(last_bar_trade_date),
                        last_bar_minute_slot = VALUES(last_bar_minute_slot),
                        last_import_mode = VALUES(last_import_mode),
                        last_status = VALUES(last_status),
                        last_rows_affected = VALUES(last_rows_affected),
                        last_error = VALUES(last_error),
                        last_finished_at = VALUES(last_finished_at)
                    """,
                    (
                        symbol_id,
                        dat_path,
                        int(file_size),
                        int(file_mtime),
                        last_trade_date,
                        last_minute_slot,
                        import_mode,
                        status,
                        rows_affected,
                        error_message,
                    ),
                )
            if own_conn:
                connection.commit()
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def create_job(self, job_type: int, symbol_count: int, note: str | None = None) -> int:
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stock_bar_import_job (job_type, status, symbol_count, note)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (job_type, 1, symbol_count, note),
                )
                job_id = int(cursor.lastrowid)
            connection.commit()
            return job_id
        except Exception:
            connection.rollback()
            raise
        finally:
            self._close_if_possible(connection)

    def update_job(self, job_id: int, **fields) -> None:
        if not fields:
            return
        columns = []
        values = []
        for key, value in fields.items():
            columns.append(f"{key} = %s")
            values.append(value)
        values.append(job_id)
        connection = self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE stock_bar_import_job SET {', '.join(columns)} WHERE job_id = %s",
                    tuple(values),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._close_if_possible(connection)

    def upsert_bar_rows(self, rows: Sequence[tuple], conn=None) -> int:
        if not rows:
            return 0
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO stock_bar_1m (
                        trade_date, minute_slot, symbol_id, open, high, low, close, volume, amount_k
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume),
                        amount_k = VALUES(amount_k)
                    """,
                    list(rows),
                )
            if own_conn:
                connection.commit()
            return len(rows)
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def get_or_create_pool(
        self,
        owner_key: str,
        pool_name: str,
        description: str | None = None,
        conn=None,
    ) -> int:
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO stock_pool (owner_key, pool_name, description)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        description = VALUES(description),
                        is_active = 1
                    """,
                    (owner_key, pool_name, description or ""),
                )
                cursor.execute(
                    "SELECT pool_id FROM stock_pool WHERE owner_key = %s AND pool_name = %s",
                    (owner_key, pool_name),
                )
                row = cursor.fetchone()
            if own_conn:
                connection.commit()
            if not row:
                raise RuntimeError("无法获取 pool_id")
            return int(row["pool_id"])
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def replace_pool_symbols(self, pool_id: int, symbol_ids: Iterable[int], conn=None) -> None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        normalized_ids = list(dict.fromkeys(int(symbol_id) for symbol_id in symbol_ids))
        try:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM stock_pool_symbol WHERE pool_id = %s", (pool_id,))
                if normalized_ids:
                    cursor.executemany(
                        """
                        INSERT INTO stock_pool_symbol (pool_id, symbol_id, sort_order)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            sort_order = VALUES(sort_order)
                        """,
                        [
                            (pool_id, symbol_id, sort_order)
                            for sort_order, symbol_id in enumerate(normalized_ids)
                        ],
                    )
            if own_conn:
                connection.commit()
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def append_pool_symbols(self, pool_id: int, symbol_ids: Iterable[int], conn=None) -> None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        normalized_ids = list(dict.fromkeys(int(symbol_id) for symbol_id in symbol_ids))
        try:
            with connection.cursor() as cursor:
                for symbol_id in normalized_ids:
                    cursor.execute(
                        """
                        INSERT INTO stock_pool_symbol (pool_id, symbol_id, sort_order)
                        VALUES (
                            %s,
                            %s,
                            COALESCE(
                                (SELECT max_sort + 1 FROM (
                                    SELECT MAX(sort_order) AS max_sort
                                    FROM stock_pool_symbol
                                    WHERE pool_id = %s
                                ) AS seq),
                                0
                            )
                        )
                        ON DUPLICATE KEY UPDATE sort_order = sort_order
                        """,
                        (pool_id, symbol_id, pool_id),
                    )
            if own_conn:
                connection.commit()
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def remove_pool_symbols(self, pool_id: int, symbol_ids: Iterable[int], conn=None) -> None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        normalized_ids = list(dict.fromkeys(int(symbol_id) for symbol_id in symbol_ids))
        if not normalized_ids:
            return
        placeholders = ", ".join(["%s"] * len(normalized_ids))
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM stock_pool_symbol WHERE pool_id = %s AND symbol_id IN ({placeholders})",
                    tuple([pool_id] + normalized_ids),
                )
            if own_conn:
                connection.commit()
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def get_pool(self, owner_key: str, pool_name: str, conn=None) -> dict | None:
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pool_id, owner_key, pool_name, description, is_active
                    FROM stock_pool
                    WHERE owner_key = %s AND pool_name = %s
                    """,
                    (owner_key, pool_name),
                )
                return cursor.fetchone()
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def list_pool_symbols(self, pool_id: int, conn=None) -> list[dict]:
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT s.symbol_id, s.symbol, p.sort_order
                    FROM stock_pool_symbol p
                    JOIN stock_symbol s ON s.symbol_id = p.symbol_id
                    WHERE p.pool_id = %s
                    ORDER BY p.sort_order ASC, s.symbol ASC
                    """,
                    (pool_id,),
                )
                return list(cursor.fetchall() or [])
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def upsert_qfq_factor_rows(self, rows: Sequence[tuple], conn=None) -> int:
        if not rows:
            return 0
        own_conn = conn is None
        connection = conn or self._get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO stock_qfq_factor (
                        symbol_id, trade_date, factor, source_file_mtime
                    ) VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        factor = VALUES(factor),
                        source_file_mtime = VALUES(source_file_mtime)
                    """,
                    list(rows),
                )
            if own_conn:
                connection.commit()
            return len(rows)
        except Exception:
            if own_conn:
                connection.rollback()
            raise
        finally:
            if own_conn:
                self._close_if_possible(connection)

    def load_qfq_factor_rows(
        self,
        symbol_ids: Sequence[int],
        start_date=None,
        end_date=None,
        conn=None,
    ) -> list[dict]:
        normalized_ids = list(dict.fromkeys(int(symbol_id) for symbol_id in symbol_ids if symbol_id))
        if not normalized_ids:
            return []

        own_conn = conn is None
        connection = conn or self._get_connection()
        placeholders = ", ".join(["%s"] * len(normalized_ids))
        conditions = [f"f.symbol_id IN ({placeholders})"]
        params: list = list(normalized_ids)
        if start_date is not None:
            conditions.append("f.trade_date >= %s")
            params.append(getattr(start_date, "date", lambda: start_date)())
        if end_date is not None:
            conditions.append("f.trade_date <= %s")
            params.append(getattr(end_date, "date", lambda: end_date)())

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        s.symbol,
                        f.symbol_id,
                        f.trade_date,
                        f.factor
                    FROM stock_qfq_factor f
                    JOIN stock_symbol s ON s.symbol_id = f.symbol_id
                    WHERE {' AND '.join(conditions)}
                    ORDER BY s.symbol ASC, f.trade_date ASC
                    """,
                    tuple(params),
                )
                return list(cursor.fetchall() or [])
        finally:
            if own_conn:
                self._close_if_possible(connection)
