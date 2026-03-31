import ast
import unittest
from pathlib import Path


class TestClickhouseBacktestDefaults(unittest.TestCase):
    def test_main_should_default_to_clickhouse_and_qfq(self):
        main_path = Path(__file__).resolve().parents[1] / "main.py"
        module = ast.parse(main_path.read_text(encoding="utf-8"))
        assignments = {}

        for node in module.body:
            if not isinstance(node, ast.Assign):
                continue
            if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
                continue
            name = node.targets[0].id
            if name in {"BAR_SOURCE", "ADJUST_MODE"} and isinstance(node.value, ast.Constant):
                assignments[name] = node.value.value

        self.assertEqual(assignments.get("BAR_SOURCE"), "clickhouse")
        self.assertEqual(assignments.get("ADJUST_MODE"), "qfq")


if __name__ == "__main__":
    unittest.main()
