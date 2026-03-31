import unittest

import pandas as pd

from customized_backtest_tool.backend.adjust_factor_importer import build_factor_rows


class TestAdjustFactorImporter(unittest.TestCase):
    def test_build_factor_rows_should_only_keep_non_null_events(self):
        frame = pd.DataFrame(
            {
                "000001.SZ": [None, 0.95, None],
                "600519.SH": [1.02, None, None],
            },
            index=pd.to_datetime(["2026-03-17", "2026-03-18", "2026-03-19"]),
        )
        symbol_map = {"000001.SZ": 1, "600519.SH": 2}

        rows = build_factor_rows(frame, ["000001.SZ", "600519.SH"], symbol_map, source_file_mtime=99)

        self.assertEqual(
            rows,
            [
                (1, pd.Timestamp("2026-03-18").date(), 0.95, 99),
                (2, pd.Timestamp("2026-03-17").date(), 1.02, 99),
            ],
        )


if __name__ == "__main__":
    unittest.main()
