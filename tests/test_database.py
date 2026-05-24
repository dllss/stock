import unittest
from unittest import mock


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql):
        self.sql = sql

    def fetchone(self):
        return (1,)


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return FakeCursor()


class FakeCursorNoTable(FakeCursor):
    def fetchone(self):
        return None


class FakeConnectionNoTable(FakeConnection):
    def cursor(self):
        return FakeCursorNoTable()


class DatabaseTest(unittest.TestCase):
    def test_check_table_exists_success_uses_debug_logging(self):
        from instock.lib import database

        with mock.patch.object(database, "get_connection", return_value=FakeConnection()), \
            mock.patch.object(database.logging, "info") as log_info, \
            mock.patch.object(database.logging, "debug") as log_debug, \
            mock.patch.object(database.logging, "warning") as log_warning:
            result = database.checkTableIsExist("cn_stock_spot")

        self.assertTrue(result)
        log_info.assert_not_called()
        self.assertEqual(log_debug.call_count, 2)
        log_warning.assert_not_called()

    def test_check_table_does_not_exist_uses_warning_logging(self):
        from instock.lib import database

        with mock.patch.object(database, "get_connection", return_value=FakeConnectionNoTable()), \
            mock.patch.object(database.logging, "info") as log_info, \
            mock.patch.object(database.logging, "debug") as log_debug, \
            mock.patch.object(database.logging, "warning") as log_warning:
            result = database.checkTableIsExist("nonexistent_table")

        self.assertFalse(result)
        log_info.assert_not_called()
        log_debug.assert_called_once()
        log_warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
