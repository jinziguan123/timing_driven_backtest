from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Sequence


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool = False


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def build_clickhouse_config_from_env() -> ClickHouseConfig:
    return ClickHouseConfig(
        host=os.environ.get("CLICKHOUSE_HOST", "172.30.26.12"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        user=os.environ.get("CLICKHOUSE_USER", "quant"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "Jinziguan123"),
        database=os.environ.get("CLICKHOUSE_DATABASE", "quant_data"),
        secure=_to_bool(os.environ.get("CLICKHOUSE_SECURE"), default=False),
    )


def _import_clickhouse_connect():
    try:
        import clickhouse_connect
    except ModuleNotFoundError as exc:
        raise RuntimeError("缺少 clickhouse-connect 依赖，请先安装 requirements.txt") from exc
    return clickhouse_connect


def connect_clickhouse(config: ClickHouseConfig | None = None):
    clickhouse_connect = _import_clickhouse_connect()
    cfg = config or build_clickhouse_config_from_env()
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.user,
        password=cfg.password,
        database=cfg.database,
        secure=cfg.secure,
    )


def ensure_database(config: ClickHouseConfig | None = None) -> None:
    cfg = config or build_clickhouse_config_from_env()
    client = connect_clickhouse(cfg)
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{cfg.database}`")
    finally:
        close_method = getattr(client, "close", None)
        if callable(close_method):
            close_method()


def init_schema(config: ClickHouseConfig | None = None) -> None:
    cfg = config or build_clickhouse_config_from_env()
    ensure_database(cfg)
    client = connect_clickhouse(cfg)
    try:
        client.command(
            """
            CREATE TABLE IF NOT EXISTS stock_bar_1m
            (
                symbol_id UInt32,
                trade_date Date,
                minute_slot UInt16,
                open Float32,
                high Float32,
                low Float32,
                close Float32,
                volume UInt32,
                amount_k UInt32,
                version UInt64,
                updated_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(version)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (symbol_id, trade_date, minute_slot)
            SETTINGS index_granularity = 8192
            """
        )
    finally:
        close_method = getattr(client, "close", None)
        if callable(close_method):
            close_method()


class ClickHouseBarStorage:
    def __init__(
        self,
        client_factory: Callable[[], object] | None = None,
        batch_size: int = 50_000,
    ):
        self._client_factory = client_factory or (lambda: connect_clickhouse())
        self.batch_size = max(1, int(batch_size))

    def _get_client(self):
        return self._client_factory()

    @staticmethod
    def _close_if_possible(client) -> None:
        close_method = getattr(client, "close", None)
        if callable(close_method):
            close_method()

    def upsert_bar_rows(
        self,
        rows: Sequence[tuple],
        *,
        version: int | None = None,
        batch_size: int | None = None,
        client=None,
    ) -> int:
        if not rows:
            return 0

        own_client = client is None
        ch_client = client or self._get_client()
        try:
            effective_batch_size = max(1, int(batch_size or self.batch_size))
            base_version = int(version or time.time_ns())
            updated_at = datetime.now().replace(microsecond=0)
            column_names = [
                "symbol_id",
                "trade_date",
                "minute_slot",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount_k",
                "version",
                "updated_at",
            ]

            affected = 0
            for start in range(0, len(rows), effective_batch_size):
                batch_rows = rows[start:start + effective_batch_size]
                payload = []
                for offset, row in enumerate(batch_rows):
                    (
                        trade_date,
                        minute_slot,
                        symbol_id,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        amount_k,
                    ) = row
                    payload.append(
                        [
                            int(symbol_id),
                            trade_date,
                            int(minute_slot),
                            float(open_price),
                            float(high_price),
                            float(low_price),
                            float(close_price),
                            max(0, int(volume)),
                            max(0, int(amount_k)),
                            base_version + start + offset,
                            updated_at,
                        ]
                    )
                ch_client.insert(
                    "stock_bar_1m",
                    payload,
                    column_names=column_names,
                )
                affected += len(batch_rows)
            return affected
        finally:
            if own_client:
                self._close_if_possible(ch_client)
