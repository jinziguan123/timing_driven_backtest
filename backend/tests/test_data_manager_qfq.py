import unittest

import pandas as pd

from customized_backtest_tool.backend.data_manager import apply_qfq_to_symbol_frame, load_data_generator


class _FakeClickHouseReader:
    def __init__(self, frame: pd.DataFrame):
        self._frame = frame

    def load_symbol_minutes(self, symbol: str, start_datetime=None, end_datetime=None):
        return self._frame.copy()


class TestDataManagerQfq(unittest.TestCase):
    def test_apply_qfq_to_symbol_frame_should_adjust_ohlc_only(self):
        frame = pd.DataFrame(
            {
                "open": [10.0, 11.0],
                "high": [10.5, 11.5],
                "low": [9.8, 10.8],
                "close": [10.2, 11.2],
                "volume": [100, 200],
                "amount": [1000, 2000],
            },
            index=pd.to_datetime(["2026-03-17 09:31:00", "2026-03-18 09:31:00"]),
        )
        factor_series = pd.Series(
            [2.0],
            index=pd.to_datetime(["2026-03-18"]),
        )

        adjusted = apply_qfq_to_symbol_frame(frame, factor_series)

        self.assertAlmostEqual(float(adjusted.iloc[0]["close"]), 5.1, places=5)
        self.assertEqual(int(adjusted.iloc[0]["volume"]), 100)
        self.assertAlmostEqual(float(adjusted.iloc[1]["close"]), 11.2, places=5)

    def test_load_data_generator_should_apply_qfq_when_source_is_clickhouse(self):
        frame = pd.DataFrame(
            {
                "open": [10.0, 11.0],
                "high": [10.5, 11.5],
                "low": [9.8, 10.8],
                "close": [10.2, 11.2],
                "volume": [100, 200],
                "amount": [1000, 2000],
            },
            index=pd.to_datetime(["2026-03-17 09:31:00", "2026-03-18 09:31:00"]),
        )
        factor_map = {
            "000001.SZ": pd.Series([2.0], index=pd.to_datetime(["2026-03-18"]))
        }

        rows = list(
            load_data_generator(
                stock_list=["000001.SZ"],
                start_date_time="2026-03-17 09:31:00",
                end_date_time="2026-03-18 09:31:00",
                source="clickhouse",
                adjust="qfq",
                clickhouse_reader=_FakeClickHouseReader(frame),
                qfq_factor_map=factor_map,
            )
        )

        self.assertEqual(len(rows), 1)
        stock, adjusted = rows[0]
        self.assertEqual(stock, "000001.SZ")
        self.assertAlmostEqual(float(adjusted.loc[pd.Timestamp("2026-03-17 09:31:00"), "close"]), 5.1, places=5)
        self.assertAlmostEqual(float(adjusted.loc[pd.Timestamp("2026-03-18 09:31:00"), "close"]), 11.2, places=5)


if __name__ == "__main__":
    unittest.main()
