from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd


MARKET_CODE_MAP = {
    "SH": 1,
    "SZ": 2,
    "BJ": 3,
}

VALID_MINUTE_SLOTS = set(range(571, 691)) | set(range(781, 901))


def compress_amount_to_k(amount: int | float) -> int:
    return max(0, int(round(float(amount) / 1000.0)))


def restore_amount_from_k(amount_k: int | float) -> float:
    return float(amount_k) * 1000.0


def normalize_symbol(symbol: str) -> str:
    code, market = symbol.strip().upper().split(".", 1)
    if market not in MARKET_CODE_MAP:
        raise ValueError(f"unsupported market: {market}")
    if len(code) != 6 or not code.isdigit():
        raise ValueError(f"unsupported symbol: {symbol}")
    return f"{code}.{market}"


def decode_market_code(symbol: str) -> tuple[int, str, str]:
    normalized = normalize_symbol(symbol)
    code, market = normalized.split(".", 1)
    return MARKET_CODE_MAP[market], code, normalized


def minute_slot_from_timestamp(ts: pd.Timestamp) -> int:
    return ts.hour * 60 + ts.minute


def minute_slot_to_time_text(minute_slot: int) -> str:
    hour = minute_slot // 60
    minute = minute_slot % 60
    return f"{hour:02d}:{minute:02d}:00"


def is_valid_minute_slot(minute_slot: int) -> bool:
    return minute_slot in VALID_MINUTE_SLOTS


def _normalize_index(index: Iterable) -> pd.DatetimeIndex:
    dt_index = pd.DatetimeIndex(pd.to_datetime(index))
    if dt_index.tz is not None:
        dt_index = dt_index.tz_convert(None)
    return dt_index.floor("min")


def normalize_symbol_bar_frame(symbol_id: int, frame: pd.DataFrame) -> list[tuple[date, int, int, float, float, float, float, int, int]]:
    if frame.empty:
        return []

    required_columns = ["open", "high", "low", "close", "volume", "amount"]
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        raise ValueError(f"missing columns: {', '.join(missing_columns)}")

    working = frame.loc[:, required_columns].copy()
    working.index = _normalize_index(working.index)
    working = working[~working.index.duplicated(keep="last")]

    working["trade_date"] = working.index.date
    working["minute_slot"] = working.index.hour * 60 + working.index.minute
    working = working[working["minute_slot"].isin(VALID_MINUTE_SLOTS)]

    if working.empty:
        return []

    amount_k = working["amount"].map(compress_amount_to_k).astype(int)
    volume = working["volume"].fillna(0).astype(int)

    return list(
        zip(
            working["trade_date"],
            working["minute_slot"].astype(int),
            [symbol_id] * len(working),
            working["open"].astype(float),
            working["high"].astype(float),
            working["low"].astype(float),
            working["close"].astype(float),
            volume,
            amount_k,
        )
    )

