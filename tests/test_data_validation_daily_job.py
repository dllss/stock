import datetime
import unittest
from unittest import mock

import pandas as pd


class DataValidationDailyJobTest(unittest.TestCase):
    def test_validate_daily_table_data_uses_given_date_in_queries(self):
        from instock.job import data_validation_daily_job as job

        queries = []

        def fake_read_sql(sql, con, params=None):
            queries.append(sql)
            params_list.append(params)
            return pd.DataFrame({"count": [3]})

        params_list = []
        with mock.patch.object(job.mdb, "checkTableIsExist", return_value=True), \
            mock.patch.object(job.mdb, "engine", return_value=object()), \
            mock.patch.object(job.pd, "read_sql", side_effect=fake_read_sql):
            job.validate_daily_table_data(datetime.date(2026, 5, 20))

        self.assertTrue(queries)
        self.assertTrue(all("2026-05-20" not in str(query) for query in queries))
        self.assertTrue(all(params == {"date": "2026-05-20"} for params in params_list))

    def test_count_table_rows_rejects_unknown_table_name(self):
        from instock.job import data_validation_daily_job as job

        with self.assertRaises(ValueError):
            job.count_table_rows("cn_stock_spot`; DROP TABLE cn_stock_spot; --", "2026-05-20")

    def test_main_delegates_date_argument_parsing_to_run_template(self):
        from instock.job import data_validation_daily_job as job

        with mock.patch.object(job.runt, "run_with_args") as run_with_args:
            job.main()

        run_with_args.assert_called_once_with(job.validate_daily_table_data)


if __name__ == "__main__":
    unittest.main()
