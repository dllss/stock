import datetime
import unittest
from unittest import mock

import pandas as pd


class AdjustmentDataDailyJobTest(unittest.TestCase):
    def test_normalize_qfq_hist_data_maps_only_kline_columns(self):
        from instock.job import adjustment_data_daily_job as job

        raw = pd.DataFrame(
            [
                {
                    "日期": "2026-05-21",
                    "开盘": "84.84",
                    "收盘": "86.10",
                    "最高": "87.20",
                    "最低": "83.90",
                    "成交量": "12345",
                    "成交额": "456789",
                    "振幅": "4.50",
                    "涨跌幅": "-7.79",
                    "涨跌额": "-7.25",
                    "换手率": "3.21",
                    "不应写入": "keep-out",
                }
            ]
        )

        result = job.normalize_qfq_hist_data(raw, "301319")

        self.assertEqual(
            list(result.columns),
            ["date", "code", *job.KLINE_UPDATE_COLUMNS],
        )
        self.assertEqual(result.loc[0, "date"], "2026-05-21")
        self.assertEqual(result.loc[0, "code"], "301319")
        self.assertEqual(result.loc[0, "new_price"], 86.10)
        self.assertNotIn("不应写入", result.columns)

    def test_build_update_params_preserves_non_kline_fields(self):
        from instock.job import adjustment_data_daily_job as job

        row = pd.Series(
            {
                "date": "2026-05-21",
                "code": "301319",
                "open_price": 84.84,
                "new_price": 86.10,
                "high_price": 87.20,
                "low_price": 83.90,
                "volume": 12345,
                "deal_amount": 456789,
                "amplitude": 4.50,
                "change_rate": -7.79,
                "ups_downs": -7.25,
                "turnoverrate": 3.21,
                "industry": "electronics",
                "total_market_cap": 999,
            }
        )

        sql = job.build_update_sql()
        params = job.build_update_params(row)

        self.assertIn("UPDATE `cn_stock_spot` SET", sql)
        self.assertIn("WHERE `code` = %s AND `date` = %s", sql)
        self.assertNotIn("industry", sql)
        self.assertNotIn("total_market_cap", sql)
        self.assertEqual(params[-2:], ("301319", "2026-05-21"))
        self.assertEqual(len(params), len(job.KLINE_UPDATE_COLUMNS) + 2)

    def test_get_repair_window_matches_stock_hist_window(self):
        from instock.job import adjustment_data_daily_job as job

        with mock.patch.object(job.trd, "get_trade_hist_interval", return_value=("20230522", True)):
            start_date, end_date = job.get_repair_window(datetime.date(2026, 5, 21))

        self.assertEqual(start_date, datetime.date(2023, 5, 22))
        self.assertEqual(end_date, datetime.date(2026, 5, 21))

    def test_get_ex_dividend_query_window_uses_recent_candidates(self):
        from instock.job import adjustment_data_daily_job as job

        start_date, end_date = job.get_ex_dividend_query_window(
            datetime.date(2023, 5, 22),
            datetime.date(2026, 5, 21),
            lookback_days=30,
        )

        self.assertEqual(start_date, datetime.date(2026, 4, 21))
        self.assertEqual(end_date, datetime.date(2026, 5, 21))

    def test_execute_daily_job_runs_adjustment_before_indicators(self):
        from instock.job import execute_daily_job

        calls = []

        def record(name):
            return lambda *args, **kwargs: calls.append(name)

        with mock.patch.object(execute_daily_job.init_job, "main", record("init")), \
            mock.patch.object(execute_daily_job.cn_stock_spot_job, "main", record("spot")), \
            mock.patch.object(execute_daily_job.cn_etf_spot_job, "main", record("etf")), \
            mock.patch.object(execute_daily_job.cn_stock_selection_job, "main", record("selection")), \
            mock.patch.object(execute_daily_job.basic_data_other_daily_job, "main", record("other")), \
            mock.patch.object(execute_daily_job.adjustment_data_daily_job, "main", record("adjustment")), \
            mock.patch.object(execute_daily_job.indicators_data_daily_job, "main", record("indicators")), \
            mock.patch.object(execute_daily_job.klinepattern_data_daily_job, "main", record("kline")), \
            mock.patch.object(execute_daily_job.strategy_data_daily_job, "main", record("strategy")), \
            mock.patch.object(execute_daily_job.backtest_data_daily_job, "prepare", record("backtest")), \
            mock.patch.object(execute_daily_job.basic_data_after_close_daily_job, "main", record("after_close")), \
            mock.patch.object(execute_daily_job, "validate_today_data", record("validate")):
            execute_daily_job.main()

        self.assertLess(calls.index("other"), calls.index("adjustment"))
        self.assertLess(calls.index("adjustment"), calls.index("indicators"))


if __name__ == "__main__":
    unittest.main()
