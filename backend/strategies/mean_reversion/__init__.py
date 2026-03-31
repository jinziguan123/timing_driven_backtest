"""
均值回归类策略

包含基于均值回归思想的交易策略：
- RSI 超买超卖策略
- 布林带策略
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from strategies.mean_reversion.rsi import RSI_Strategy

__all__ = ['RSI_Strategy']

