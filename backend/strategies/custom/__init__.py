"""
自定义策略模块

存放测试策略和其他自定义策略。
"""

import sys
from pathlib import Path

# 添加 backend 目录到路径
backend_dir = Path(__file__).resolve().parent.parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

from strategies.custom.test_strategy import Test_Strategy

__all__ = ['Test_Strategy']

