"""
Numba 加速函数模块

存放使用 Numba JIT 编译的高性能计算函数。
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from signals._numba.grid_simulator import numba_grid_simulator
from signals._numba.sell_signals import (
    numba_generate_bias_macd_jinzuan_sell_signal,
    numba_generate_bias_expma_sell_signal,
    numba_generate_week_ema_sell_signal,
    numba_generate_fibonacci_ema_sell_signal,
    numba_generate_fibonacci_ema_multi_position_signals,
    numba_generate_fibonacci_ema_signal_anchor_order_matrices,
    numba_r_breaker_signals,
)

__all__ = [
    'numba_grid_simulator',
    'numba_generate_bias_macd_jinzuan_sell_signal',
    'numba_generate_bias_expma_sell_signal',
    'numba_generate_week_ema_sell_signal',
    'numba_generate_fibonacci_ema_sell_signal',
    'numba_generate_fibonacci_ema_multi_position_signals',
    'numba_generate_fibonacci_ema_signal_anchor_order_matrices',
    'numba_r_breaker_signals',
]
