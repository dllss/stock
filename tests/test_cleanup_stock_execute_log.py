import datetime
import os
import tempfile
import unittest


class CleanupStockExecuteLogTest(unittest.TestCase):
    def test_cleanup_keeps_cutoff_date_and_later_log_blocks(self):
        from instock.job import cleanup_stock_execute_log as cleaner

        content = (
            "2026-05-19 23:59:59,000 [old.py:1 - run()] old log\n"
            "old traceback continuation\n"
            "2026-05-20 00:00:00,000 [job.py:2 - run()] boundary log\n"
            "boundary traceback continuation\n"
            "2026-05-21 09:00:00,000 [job.py:3 - run()] new log\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "stock_execute_job.log")
            backup_path = os.path.join(tmpdir, "stock_execute_job.log.bak")
            with open(log_path, "w", encoding="utf-8") as file:
                file.write(content)

            result = cleaner.cleanup_log_before_date(
                log_path,
                datetime.date(2026, 5, 20),
                backup_path=backup_path,
            )

            with open(log_path, "r", encoding="utf-8") as file:
                cleaned = file.read()
            with open(backup_path, "r", encoding="utf-8") as file:
                backup = file.read()

        self.assertEqual(result.removed_lines, 2)
        self.assertEqual(result.kept_lines, 3)
        self.assertNotIn("old log", cleaned)
        self.assertNotIn("old traceback continuation", cleaned)
        self.assertIn("boundary log", cleaned)
        self.assertIn("boundary traceback continuation", cleaned)
        self.assertIn("new log", cleaned)
        self.assertEqual(backup, content)

    def test_dry_run_does_not_modify_log_or_create_backup(self):
        from instock.job import cleanup_stock_execute_log as cleaner

        content = (
            "2026-05-19 23:59:59,000 [old.py:1 - run()] old log\n"
            "2026-05-20 00:00:00,000 [job.py:2 - run()] boundary log\n"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "stock_execute_job.log")
            backup_path = os.path.join(tmpdir, "stock_execute_job.log.bak")
            with open(log_path, "w", encoding="utf-8") as file:
                file.write(content)

            result = cleaner.cleanup_log_before_date(
                log_path,
                datetime.date(2026, 5, 20),
                backup_path=backup_path,
                dry_run=True,
            )

            with open(log_path, "r", encoding="utf-8") as file:
                after = file.read()
            backup_exists = os.path.exists(backup_path)

        self.assertEqual(result.removed_lines, 1)
        self.assertEqual(result.kept_lines, 1)
        self.assertEqual(after, content)
        self.assertFalse(backup_exists)

    def test_no_removed_lines_does_not_report_backup(self):
        from instock.job import cleanup_stock_execute_log as cleaner

        content = "2026-05-20 00:00:00,000 [job.py:2 - run()] boundary log\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = os.path.join(tmpdir, "stock_execute_job.log")
            backup_path = os.path.join(tmpdir, "stock_execute_job.log.bak")
            with open(log_path, "w", encoding="utf-8") as file:
                file.write(content)

            result = cleaner.cleanup_log_before_date(
                log_path,
                datetime.date(2026, 5, 20),
                backup_path=backup_path,
            )

            backup_exists = os.path.exists(backup_path)

        self.assertEqual(result.removed_lines, 0)
        self.assertIsNone(result.backup_path)
        self.assertFalse(backup_exists)

    def test_malformed_timestamp_line_uses_previous_block_state(self):
        from instock.job import cleanup_stock_execute_log as cleaner

        lines = [
            "2026-05-19 23:59:59,000 [old.py:1 - run()] old log\n",
            "2026-99-99 00:00:00,000 [bad.py:1 - run()] malformed timestamp\n",
            "2026-05-20 00:00:00,000 [job.py:2 - run()] boundary log\n",
        ]

        kept_lines, removed_lines = cleaner.split_lines_by_cutoff(lines, datetime.date(2026, 5, 20))

        self.assertEqual(removed_lines, 2)
        self.assertEqual(kept_lines, ["2026-05-20 00:00:00,000 [job.py:2 - run()] boundary log\n"])


if __name__ == "__main__":
    unittest.main()
