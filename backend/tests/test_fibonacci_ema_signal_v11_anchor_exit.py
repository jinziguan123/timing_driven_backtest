import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


def _identity_njit(*args, **kwargs):
    if args and callable(args[0]) and len(args) == 1 and not kwargs:
        return args[0]

    def decorator(func):
        return func

    return decorator


if "numba" not in sys.modules:
    fake_numba = types.ModuleType("numba")
    fake_numba.njit = _identity_njit
    sys.modules["numba"] = fake_numba

backend_dir = Path(__file__).resolve().parents[1]
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

if "data_manager" not in sys.modules:
    fake_data_manager = types.ModuleType("data_manager")
    fake_data_manager.merge_data = lambda *args, **kwargs: None
    sys.modules["data_manager"] = fake_data_manager

if "signals" not in sys.modules:
    fake_signals = types.ModuleType("signals")
    fake_signals.__path__ = []
    sys.modules["signals"] = fake_signals

if "signals._numba" not in sys.modules:
    fake_numba_signals = types.ModuleType("signals._numba")
    fake_numba_signals.numba_generate_fibonacci_ema_sell_signal = lambda *args, **kwargs: None
    fake_numba_signals.numba_generate_fibonacci_ema_multi_position_signals = (
        lambda *args, **kwargs: None
    )
    fake_numba_signals.numba_generate_fibonacci_ema_signal_anchor_order_matrices = (
        lambda *args, **kwargs: (None, None)
    )
    sys.modules["signals._numba"] = fake_numba_signals

module_name = "fibonacci_ema_under_test_v11_anchor"
module_path = backend_dir / "signals" / "technical" / "fibonacci_ema.py"
spec = importlib.util.spec_from_file_location(module_name, module_path)
mod = importlib.util.module_from_spec(spec)
sys.modules[module_name] = mod
assert spec.loader is not None
spec.loader.exec_module(mod)

sell_signals_module_name = "sell_signals_under_test_v11_anchor"
sell_signals_module_path = backend_dir / "signals" / "_numba" / "sell_signals.py"
sell_signals_spec = importlib.util.spec_from_file_location(
    sell_signals_module_name, sell_signals_module_path
)
sell_signals_under_test = importlib.util.module_from_spec(sell_signals_spec)
sys.modules[sell_signals_module_name] = sell_signals_under_test
assert sell_signals_spec.loader is not None
sell_signals_spec.loader.exec_module(sell_signals_under_test)
REAL_MULTI_POSITION_FN = (
    sell_signals_under_test.numba_generate_fibonacci_ema_multi_position_signals
)
ANCHOR_ORDER_FN = getattr(
    sell_signals_under_test,
    "numba_generate_fibonacci_ema_signal_anchor_order_matrices",
    None,
)
if ANCHOR_ORDER_FN is not None:
    setattr(mod, "numba_generate_fibonacci_ema_signal_anchor_order_matrices", ANCHOR_ORDER_FN)

Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11 = getattr(
    mod,
    "Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11",
    None,
)


class TestFibonacciEMASignalV11AnchorExit(unittest.TestCase):
    def test_anchor_order_numba_function_should_live_in_sell_signals_module(self):
        self.assertIsNotNone(
            ANCHOR_ORDER_FN,
            "V11 专用 numba 函数应当定义在 sell_signals.py 中",
        )

    def setUp(self):
        self.symbol = "000001.SZ"
        self.day_index = pd.bdate_range("2024-01-02", periods=260)
        self.signal_day = self.day_index[200]
        self.exec_day = self.signal_day + pd.offsets.BDay(1)
        self.exit_day = self.signal_day + pd.offsets.BDay(2)

        minute_timestamps = []
        for day in self.day_index:
            minute_timestamps.extend(
                [
                    day + pd.Timedelta(hours=9, minutes=31),
                    day + pd.Timedelta(hours=10, minutes=0),
                    day + pd.Timedelta(hours=10, minutes=1),
                    day + pd.Timedelta(hours=14, minutes=30),
                ]
            )
        self.minute_index = pd.DatetimeIndex(minute_timestamps)

        self.minute_open = pd.DataFrame(100.0, index=self.minute_index, columns=[self.symbol])
        self.minute_close = pd.DataFrame(100.0, index=self.minute_index, columns=[self.symbol])
        self.minute_low = pd.DataFrame(100.0, index=self.minute_index, columns=[self.symbol])
        self.minute_high = pd.DataFrame(101.0, index=self.minute_index, columns=[self.symbol])
        self.minute_amount = pd.DataFrame(1_000_000.0, index=self.minute_index, columns=[self.symbol])
        self.minute_volume = pd.DataFrame(1_000.0, index=self.minute_index, columns=[self.symbol])

        self.day_data_dict = {
            "close": pd.DataFrame(100.0, index=self.day_index, columns=[self.symbol]),
            "open": pd.DataFrame(100.0, index=self.day_index, columns=[self.symbol]),
            "low": pd.DataFrame(99.0, index=self.day_index, columns=[self.symbol]),
            "high": pd.DataFrame(101.0, index=self.day_index, columns=[self.symbol]),
            "amount": pd.DataFrame(1_000_000.0, index=self.day_index, columns=[self.symbol]),
            "volume": pd.DataFrame(1_000.0, index=self.day_index, columns=[self.symbol]),
        }

        self.week_index = pd.date_range(self.day_index.min(), periods=60, freq="W-FRI")
        self.week_data_dict = {
            "close": pd.DataFrame(100.0, index=self.week_index, columns=[self.symbol]),
            "open": pd.DataFrame(100.0, index=self.week_index, columns=[self.symbol]),
            "low": pd.DataFrame(95.0, index=self.week_index, columns=[self.symbol]),
            "high": pd.DataFrame(105.0, index=self.week_index, columns=[self.symbol]),
            "amount": pd.DataFrame(1_000_000.0, index=self.week_index, columns=[self.symbol]),
            "volume": pd.DataFrame(1_000.0, index=self.week_index, columns=[self.symbol]),
        }

    def _build_input_data(self):
        return {
            "close": self.minute_close.copy(),
            "open": self.minute_open.copy(),
            "low": self.minute_low.copy(),
            "high": self.minute_high.copy(),
            "amount": self.minute_amount.copy(),
            "volume": self.minute_volume.copy(),
        }

    def _fake_merge_data(self, _data_dict, freq, *args, **kwargs):
        if freq == "1D":
            return self.day_data_dict
        if freq == "1W":
            return self.week_data_dict
        raise ValueError(f"unexpected freq: {freq}")

    @staticmethod
    def _fake_ema(data, period):
        if period in (13, 2):
            return pd.DataFrame(3.0, index=data.index, columns=data.columns)
        if period in (21, 20):
            return pd.DataFrame(2.0, index=data.index, columns=data.columns)
        if period == 34:
            return pd.DataFrame(1.0, index=data.index, columns=data.columns)
        return pd.DataFrame(1.0, index=data.index, columns=data.columns)

    @staticmethod
    def _fake_slope(data, period):
        return pd.DataFrame(0.0, index=data.index, columns=data.columns)

    def _fake_cross_signal_day(self, _a, _b):
        out = pd.DataFrame(False, index=self.day_index, columns=[self.symbol])
        out.loc[self.signal_day, self.symbol] = True
        return out

    @staticmethod
    def _fake_ma(data, period):
        return pd.DataFrame(110.0, index=data.index, columns=data.columns)

    @patch("fibonacci_ema_under_test_v11_anchor.numba_generate_fibonacci_ema_multi_position_signals")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.MA")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.CROSS")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.SLOPE")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.EMA")
    @patch("fibonacci_ema_under_test_v11_anchor.merge_data")
    def test_v11_stop_loss_should_use_signal_day_close_as_anchor(
        self,
        mock_merge_data,
        mock_ema,
        mock_slope,
        mock_cross,
        mock_ma,
        mock_numba,
    ):
        self.assertIsNotNone(
            Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11,
            "缺少 V11 信号生成器实现",
        )

        mock_merge_data.side_effect = self._fake_merge_data
        mock_ema.side_effect = self._fake_ema
        mock_slope.side_effect = self._fake_slope
        mock_cross.side_effect = self._fake_cross_signal_day
        mock_ma.side_effect = self._fake_ma
        mock_numba.side_effect = REAL_MULTI_POSITION_FN

        data_dict = self._build_input_data()
        trigger_ts = self.exec_day + pd.Timedelta(hours=10, minutes=0)
        buy_ts = self.exec_day + pd.Timedelta(hours=10, minutes=1)
        stop_loss_ts = self.exit_day + pd.Timedelta(hours=10, minutes=0)

        data_dict["close"].loc[trigger_ts, self.symbol] = 96.5
        data_dict["close"].loc[buy_ts, self.symbol] = 96.5
        data_dict["close"].loc[stop_loss_ts, self.symbol] = 89.0

        generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(
            params={"buy_size": 100, "take_profit": 0.1, "stop_loss": 0.1, "max_positions": 1}
        )
        _, signal_output = generator.run_simulation(data_dict, self.minute_index)

        size_df = signal_output["size"]
        price_df = signal_output["price"]
        stop_loss_exec_ts = self.exit_day + pd.Timedelta(hours=10, minutes=1)

        self.assertEqual(int(size_df.loc[buy_ts, self.symbol]), 100)
        self.assertEqual(
            int(size_df.loc[stop_loss_exec_ts, self.symbol]),
            -100,
            "止损应当以信号日收盘价 100 的 90% 为锚点，而不是以实际买入价 96.5 的 90% 为锚点",
        )
        self.assertEqual(float(price_df.loc[stop_loss_exec_ts, self.symbol]), 89.0)

    @patch("fibonacci_ema_under_test_v11_anchor.numba_generate_fibonacci_ema_multi_position_signals")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.MA")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.CROSS")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.SLOPE")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.EMA")
    @patch("fibonacci_ema_under_test_v11_anchor.merge_data")
    def test_v11_take_profit_should_use_signal_day_close_as_anchor(
        self,
        mock_merge_data,
        mock_ema,
        mock_slope,
        mock_cross,
        mock_ma,
        mock_numba,
    ):
        self.assertIsNotNone(
            Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11,
            "缺少 V11 信号生成器实现",
        )

        mock_merge_data.side_effect = self._fake_merge_data
        mock_ema.side_effect = self._fake_ema
        mock_slope.side_effect = self._fake_slope
        mock_cross.side_effect = self._fake_cross_signal_day
        mock_ma.side_effect = self._fake_ma
        mock_numba.side_effect = REAL_MULTI_POSITION_FN

        data_dict = self._build_input_data()
        trigger_ts = self.exec_day + pd.Timedelta(hours=10, minutes=0)
        buy_ts = self.exec_day + pd.Timedelta(hours=10, minutes=1)
        premature_take_profit_ts = self.exit_day + pd.Timedelta(hours=10, minutes=0)

        data_dict["close"].loc[trigger_ts, self.symbol] = 96.5
        data_dict["close"].loc[buy_ts, self.symbol] = 96.5
        data_dict["close"].loc[premature_take_profit_ts, self.symbol] = 107.0

        generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(
            params={"buy_size": 100, "take_profit": 0.1, "stop_loss": 0.1, "max_positions": 1}
        )
        _, signal_output = generator.run_simulation(data_dict, self.minute_index)

        size_df = signal_output["size"]

        self.assertEqual(int(size_df.loc[buy_ts, self.symbol]), 100)
        self.assertEqual(
            int(size_df.loc[premature_take_profit_ts, self.symbol]),
            0,
            "止盈应当以信号日收盘价 100 的 110% 为锚点，因此 107 不应提前止盈",
        )

    @patch("fibonacci_ema_under_test_v11_anchor.numba_generate_fibonacci_ema_multi_position_signals")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.MA")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.CROSS")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.SLOPE")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.EMA")
    @patch("fibonacci_ema_under_test_v11_anchor.merge_data")
    def test_v11_should_not_reenter_after_exit_within_same_signal_window(
        self,
        mock_merge_data,
        mock_ema,
        mock_slope,
        mock_cross,
        mock_ma,
        mock_numba,
    ):
        self.assertIsNotNone(
            Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11,
            "缺少 V11 信号生成器实现",
        )

        mock_merge_data.side_effect = self._fake_merge_data
        mock_ema.side_effect = self._fake_ema
        mock_slope.side_effect = self._fake_slope
        mock_cross.side_effect = self._fake_cross_signal_day
        mock_ma.side_effect = self._fake_ma
        mock_numba.side_effect = REAL_MULTI_POSITION_FN

        data_dict = self._build_input_data()
        trigger_ts = self.exec_day + pd.Timedelta(hours=10, minutes=0)
        buy_ts = self.exec_day + pd.Timedelta(hours=10, minutes=1)
        stop_loss_trigger_ts = self.exit_day + pd.Timedelta(hours=10, minutes=0)
        stop_loss_exec_ts = self.exit_day + pd.Timedelta(hours=10, minutes=1)
        post_exit_reentry_ts = self.exit_day + pd.Timedelta(hours=14, minutes=30)

        data_dict["close"].loc[trigger_ts, self.symbol] = 96.5
        data_dict["close"].loc[buy_ts, self.symbol] = 96.5
        data_dict["close"].loc[stop_loss_trigger_ts, self.symbol] = 89.0
        data_dict["close"].loc[post_exit_reentry_ts, self.symbol] = 88.5

        generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(
            params={"buy_size": 100, "take_profit": 0.1, "stop_loss": 0.1, "max_positions": 1}
        )
        _, signal_output = generator.run_simulation(data_dict, self.minute_index)

        size_df = signal_output["size"]

        self.assertEqual(int(size_df.loc[buy_ts, self.symbol]), 100)
        self.assertEqual(int(size_df.loc[stop_loss_exec_ts, self.symbol]), -100)
        self.assertEqual(
            int(size_df.loc[post_exit_reentry_ts, self.symbol]),
            0,
            "同一个三日窗口内，止损/止盈后不应再次买回",
        )

    @patch("fibonacci_ema_under_test_v11_anchor.numba_generate_fibonacci_ema_multi_position_signals")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.MA")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.CROSS")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.SLOPE")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.EMA")
    @patch("fibonacci_ema_under_test_v11_anchor.merge_data")
    def test_v11_exit_should_execute_on_next_bar_with_trigger_bar_close_price(
        self,
        mock_merge_data,
        mock_ema,
        mock_slope,
        mock_cross,
        mock_ma,
        mock_numba,
    ):
        self.assertIsNotNone(
            Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11,
            "缺少 V11 信号生成器实现",
        )

        mock_merge_data.side_effect = self._fake_merge_data
        mock_ema.side_effect = self._fake_ema
        mock_slope.side_effect = self._fake_slope
        mock_cross.side_effect = self._fake_cross_signal_day
        mock_ma.side_effect = self._fake_ma
        mock_numba.side_effect = REAL_MULTI_POSITION_FN

        data_dict = self._build_input_data()
        trigger_ts = self.exec_day + pd.Timedelta(hours=10, minutes=0)
        buy_ts = self.exec_day + pd.Timedelta(hours=10, minutes=1)
        stop_loss_trigger_ts = self.exit_day + pd.Timedelta(hours=10, minutes=0)
        stop_loss_exec_ts = self.exit_day + pd.Timedelta(hours=10, minutes=1)

        data_dict["close"].loc[trigger_ts, self.symbol] = 96.5
        data_dict["close"].loc[buy_ts, self.symbol] = 96.5
        data_dict["close"].loc[stop_loss_trigger_ts, self.symbol] = 89.0

        generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(
            params={"buy_size": 100, "take_profit": 0.1, "stop_loss": 0.1, "max_positions": 1}
        )
        _, signal_output = generator.run_simulation(data_dict, self.minute_index)

        size_df = signal_output["size"]
        price_df = signal_output["price"]

        self.assertEqual(int(size_df.loc[stop_loss_trigger_ts, self.symbol]), 0)
        self.assertEqual(int(size_df.loc[stop_loss_exec_ts, self.symbol]), -100)
        self.assertEqual(
            float(price_df.loc[stop_loss_exec_ts, self.symbol]),
            89.0,
            "卖出价格应当对应触发止损的那根分钟收盘价，而不是再往前错一根",
        )

    @patch("fibonacci_ema_under_test_v11_anchor.numba_generate_fibonacci_ema_multi_position_signals")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.MA")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.CROSS")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.SLOPE")
    @patch("fibonacci_ema_under_test_v11_anchor.data_utils.EMA")
    @patch("fibonacci_ema_under_test_v11_anchor.merge_data")
    def test_v11_should_not_sell_position_on_buy_day_even_if_take_profit_is_hit(
        self,
        mock_merge_data,
        mock_ema,
        mock_slope,
        mock_cross,
        mock_ma,
        mock_numba,
    ):
        self.assertIsNotNone(
            Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11,
            "缺少 V11 信号生成器实现",
        )

        mock_merge_data.side_effect = self._fake_merge_data
        mock_ema.side_effect = self._fake_ema
        mock_slope.side_effect = self._fake_slope
        mock_cross.side_effect = self._fake_cross_signal_day
        mock_ma.side_effect = self._fake_ma
        mock_numba.side_effect = REAL_MULTI_POSITION_FN

        data_dict = self._build_input_data()
        trigger_ts = self.exec_day + pd.Timedelta(hours=10, minutes=0)
        buy_ts = self.exec_day + pd.Timedelta(hours=10, minutes=1)
        same_day_tp_trigger_ts = self.exec_day + pd.Timedelta(hours=14, minutes=30)
        next_day_tp_trigger_ts = self.exit_day + pd.Timedelta(hours=9, minutes=31)
        next_day_tp_exec_ts = self.exit_day + pd.Timedelta(hours=10, minutes=0)

        data_dict["close"].loc[trigger_ts, self.symbol] = 96.5
        data_dict["close"].loc[buy_ts, self.symbol] = 96.5
        data_dict["close"].loc[same_day_tp_trigger_ts, self.symbol] = 111.0
        data_dict["close"].loc[next_day_tp_trigger_ts, self.symbol] = 111.0

        generator = Fibonacci_EMA_BIAS_Grab_Main_Up_Wave_SignalGenerator_V11(
            params={"buy_size": 100, "take_profit": 0.1, "stop_loss": 0.1, "max_positions": 1}
        )
        _, signal_output = generator.run_simulation(data_dict, self.minute_index)

        size_df = signal_output["size"]
        price_df = signal_output["price"]

        self.assertEqual(int(size_df.loc[buy_ts, self.symbol]), 100)
        self.assertEqual(
            int(size_df.loc[same_day_tp_trigger_ts, self.symbol]),
            0,
            "A股 T+1 下，买入当天即使达到止盈位也不应卖出",
        )
        self.assertEqual(
            int(size_df.loc[next_day_tp_exec_ts, self.symbol]),
            -100,
            "允许卖出的最早时点应为下一交易日重新触发卖出条件后的执行 bar",
        )
        self.assertEqual(float(price_df.loc[next_day_tp_exec_ts, self.symbol]), 111.0)


if __name__ == "__main__":
    unittest.main()
