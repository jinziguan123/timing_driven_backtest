import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

backend_dir = Path(__file__).resolve().parents[1]
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

if "vectorbt" not in sys.modules:
    sys.modules["vectorbt"] = types.ModuleType("vectorbt")

existing_data_manager = sys.modules.get("data_manager")
if existing_data_manager is not None and not hasattr(existing_data_manager, "load_stock_minutes"):
    sys.modules.pop("data_manager", None)

from result_saver import ResultSaver


class TestResultSaverMetadata(unittest.TestCase):
    def test_save_aggregated_result_should_persist_bar_source_and_adjust_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            saver = ResultSaver(save_dir=tmpdir, enable_cache=False)
            result_id = saver.save_aggregated_result(
                aggregated_data={
                    "stats_dict": {"000001.SZ": {"Total Return [%]": 1.0}},
                    "equity_df_list": [pd.DataFrame({"000001.SZ": [1000000.0]}, index=pd.to_datetime(["2026-03-17"]))],
                    "orders_df_list": [],
                    "trades_df_list": [],
                    "positions_df_list": [],
                    "symbols_list": ["000001.SZ"],
                },
                strategy_name="Test",
                init_cash=1000000,
                additional_info={
                    "bar_source": "clickhouse",
                    "adjust_mode": "qfq",
                    "adjust_factor_source": "mysql",
                },
            )

            with open(f"{tmpdir}/{result_id}/metadata.json", "r", encoding="utf-8") as fh:
                metadata = json.load(fh)

            self.assertEqual(metadata["bar_source"], "clickhouse")
            self.assertEqual(metadata["adjust_mode"], "qfq")
            self.assertEqual(metadata["adjust_factor_source"], "mysql")

    @patch("result_saver.load_stock_minutes")
    def test_load_minute_ohlcv_should_use_metadata_source_and_adjust_mode(self, mock_load_stock_minutes):
        with tempfile.TemporaryDirectory() as tmpdir:
            result_dir = f"{tmpdir}/demo_result"
            saver = ResultSaver(save_dir=tmpdir, enable_cache=False)
            Path(result_dir).mkdir(parents=True, exist_ok=True)

            metadata = {
                "result_id": "demo_result",
                "strategy_name": "Test",
                "timestamp": "20260329_120000",
                "stock_list": ["000001.SZ"],
                "start_time": "2026-03-17 09:31:00",
                "end_time": "2026-03-18 15:00:00",
                "bar_source": "clickhouse",
                "adjust_mode": "qfq",
                "adjust_factor_source": "mysql",
            }
            with open(f"{result_dir}/metadata.json", "w", encoding="utf-8") as fh:
                json.dump(metadata, fh, ensure_ascii=False, indent=2)

            mock_load_stock_minutes.return_value = pd.DataFrame(
                {
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.8],
                    "close": [10.2],
                    "volume": [100],
                    "amount": [1000],
                },
                index=pd.to_datetime(["2026-03-17 09:31:00"]),
            )

            saver.load_minute_ohlcv("demo_result", "000001.SZ", date="2026-03-17")

            _, kwargs = mock_load_stock_minutes.call_args
            self.assertEqual(kwargs["source"], "clickhouse")
            self.assertEqual(kwargs["adjust"], "qfq")


if __name__ == "__main__":
    unittest.main()
