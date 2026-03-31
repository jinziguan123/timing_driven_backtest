"""
网格交易类策略

包含基于网格交易思想的策略：
- 经典网格策略
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from strategies.grid.grid_strategy import Grid_Strategy

__all__ = ['Grid_Strategy']

