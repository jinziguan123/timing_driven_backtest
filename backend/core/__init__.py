"""
核心模块 - 存放基础抽象类和接口定义
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from core.base_strategy import BaseStrategy
from core.base_signal import BaseSignalGenerator

__all__ = ['BaseStrategy', 'BaseSignalGenerator']

