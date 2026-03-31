import unittest

from customized_backtest_tool.backend.qfq_factor_reader import rows_to_factor_map


class TestQfqFactorReader(unittest.TestCase):
    def test_rows_to_factor_map_should_group_by_symbol(self):
        rows = [
            {"symbol": "000001.SZ", "trade_date": "2026-03-18", "factor": 0.95},
            {"symbol": "600519.SH", "trade_date": "2026-03-17", "factor": 1.02},
        ]

        factor_map = rows_to_factor_map(rows)

        self.assertEqual(list(factor_map["000001.SZ"].index.astype(str)), ["2026-03-18"])
        self.assertEqual(float(factor_map["000001.SZ"].iloc[0]), 0.95)


if __name__ == "__main__":
    unittest.main()
