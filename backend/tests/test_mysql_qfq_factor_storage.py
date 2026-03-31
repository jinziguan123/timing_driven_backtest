import unittest
from unittest.mock import MagicMock

from customized_backtest_tool.backend.mysql_bar_schema import build_schema_sql
from customized_backtest_tool.backend.mysql_bar_storage import MysqlBarStorage


class TestMysqlQfqFactorStorage(unittest.TestCase):
    def test_schema_should_contain_stock_qfq_factor(self):
        sql = build_schema_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS stock_qfq_factor", sql)

    def test_upsert_qfq_factor_rows_should_execute_batch_write(self):
        cursor = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        storage = MysqlBarStorage(lambda: conn)

        affected = storage.upsert_qfq_factor_rows(
            [
                (1, "2026-03-17", 0.95, 123456),
                (1, "2026-03-18", 0.98, 123456),
            ]
        )

        self.assertEqual(affected, 2)
        cursor.executemany.assert_called_once()
        conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
