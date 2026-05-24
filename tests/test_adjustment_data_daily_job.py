import datetime
import importlib
import unittest
from unittest import mock

import pandas as pd


class AdjustmentDataDailyJobTest(unittest.TestCase):
    def test_invalid_ex_dividend_lookback_env_falls_back_to_default(self):
        from instock.job import adjustment_data_daily_job as job

        with mock.patch.dict("os.environ", {"INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS": "bad-value"}):
            reloaded_job = importlib.reload(job)

        self.assertEqual(reloaded_job.EX_DIVIDEND_LOOKBACK_DAYS, 30)
        importlib.reload(reloaded_job)

    def test_default_ex_dividend_query_window_uses_30_days(self):
        from instock.job import adjustment_data_daily_job as job

        start_date, end_date = job.get_ex_dividend_query_window(
            datetime.date(2023, 5, 22),
            datetime.date(2026, 5, 21),
        )

        self.assertEqual(start_date, datetime.date(2026, 4, 21))
        self.assertEqual(end_date, datetime.date(2026, 5, 21))

    def test_repair_uses_explicit_ex_dividend_window_when_provided(self):
        from instock.job import adjustment_data_daily_job as job

        with mock.patch.object(
            job,
            "get_repair_window",
            return_value=(datetime.date(2023, 5, 22), datetime.date(2026, 5, 18)),
        ), mock.patch.object(
            job,
            "get_ex_dividend_query_window",
            side_effect=AssertionError("default lookback window should not be used"),
        ), mock.patch.object(job, "get_ex_dividend_stocks", return_value=[]) as get_stocks:
            job.repair_ex_dividend_kline_data(
                datetime.date(2026, 5, 18),
                ex_dividend_start_date=datetime.date(2026, 5, 15),
                ex_dividend_end_date=datetime.date(2026, 5, 18),
            )

        get_stocks.assert_called_once_with(datetime.date(2026, 5, 15), datetime.date(2026, 5, 18))

    def test_adjustment_main_passes_explicit_window_to_run_template(self):
        from instock.job import adjustment_data_daily_job as job

        with mock.patch.object(job.runt, "run_with_args") as run_with_args:
            job.main(
                ex_dividend_start_date=datetime.date(2026, 5, 15),
                ex_dividend_end_date=datetime.date(2026, 5, 18),
            )

        run_with_args.assert_called_once_with(
            job.repair_ex_dividend_kline_data,
            datetime.date(2026, 5, 15),
            datetime.date(2026, 5, 18),
        )

    def test_execute_daily_adjustment_passes_previous_trade_date_window(self):
        from instock.job import execute_daily_job

        with mock.patch.object(
            execute_daily_job.trd,
            "get_previous_trade_date",
            return_value=datetime.date(2026, 5, 15),
        ), mock.patch.object(
            execute_daily_job.adjustment_data_daily_job,
            "repair_ex_dividend_kline_data",
        ) as repair:
            execute_daily_job.repair_adjustment_kline_for_daily_date(datetime.date(2026, 5, 18))

        repair.assert_called_once_with(
            datetime.date(2026, 5, 18),
            ex_dividend_start_date=datetime.date(2026, 5, 15),
            ex_dividend_end_date=datetime.date(2026, 5, 18),
        )

    def test_execute_daily_job_runs_explicit_window_adjustment_before_indicators(self):
        from instock.job import execute_daily_job

        calls = []

        def record(name):
            return lambda *args, **kwargs: calls.append((name, args, kwargs))

        with mock.patch.object(execute_daily_job.init_job, "main", record("init")), \
            mock.patch.object(execute_daily_job.cn_stock_spot_job, "main", record("spot")), \
            mock.patch.object(execute_daily_job.cn_etf_spot_job, "main", record("etf")), \
            mock.patch.object(execute_daily_job.cn_stock_selection_job, "main", record("selection")), \
            mock.patch.object(execute_daily_job.basic_data_other_daily_job, "main", record("other")), \
            mock.patch.object(execute_daily_job, "run_adjustment_kline_repair_for_daily_job", record("adjustment")), \
            mock.patch.object(execute_daily_job.indicators_data_daily_job, "main", record("indicators")), \
            mock.patch.object(execute_daily_job.klinepattern_data_daily_job, "main", record("kline")), \
            mock.patch.object(execute_daily_job.strategy_data_daily_job, "main", record("strategy")), \
            mock.patch.object(execute_daily_job.backtest_data_daily_job, "prepare", record("backtest")), \
            mock.patch.object(execute_daily_job.basic_data_after_close_daily_job, "main", record("after_close")), \
            mock.patch.object(execute_daily_job.data_validation_daily_job, "main", record("validate")):
            execute_daily_job.main()

        self.assertLess(calls.index(("other", (), {})), calls.index(("adjustment", (), {})))
        self.assertLess(calls.index(("adjustment", (), {})), calls.index(("indicators", (), {})))

    def test_calculate_only_uses_explicit_window_adjustment(self):
        from instock.job import execute_daily_job

        with mock.patch.object(execute_daily_job, "run_adjustment_kline_repair_for_daily_job") as run_adjustment, \
            mock.patch.object(execute_daily_job.indicators_data_daily_job, "main"), \
            mock.patch.object(execute_daily_job.klinepattern_data_daily_job, "main"), \
            mock.patch.object(execute_daily_job.strategy_data_daily_job, "main"), \
            mock.patch.object(execute_daily_job.backtest_data_daily_job, "prepare"):
            execute_daily_job.main_calculate_only()

        run_adjustment.assert_called_once_with()

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

    def test_fetch_qfq_hist_data_bypasses_code_map_cache(self):
        from instock.job import adjustment_data_daily_job as job

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "klines": [
                            "2026-05-20,84.84,86.10,87.20,83.90,12345,456789,4.50,-7.79,-7.25,3.21"
                        ]
                    }
                }

        with mock.patch.object(job.stock_hist_em, "stock_zh_a_hist", side_effect=AssertionError), \
            mock.patch.object(job.stock_hist_em.fetcher, "make_request", return_value=FakeResponse()) as make_request:
            result = job.fetch_qfq_hist_data(
                "300995",
                datetime.date(2023, 5, 21),
                datetime.date(2026, 5, 20),
            )

        self.assertEqual(result.loc[0, "日期"], "2026-05-20")
        self.assertEqual(result.loc[0, "收盘"], "86.10")
        request_params = make_request.call_args.kwargs["params"]
        self.assertEqual(request_params["secid"], "0.300995")
        self.assertEqual(request_params["fqt"], "1")
        self.assertEqual(request_params["fields2"], "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116")

    def test_fetch_qfq_hist_data_accepts_optional_f116_column(self):
        from instock.job import adjustment_data_daily_job as job

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "klines": [
                            "2026-05-20,84.84,86.10,87.20,83.90,12345,456789,4.50,-7.79,-7.25,3.21,extra"
                        ]
                    }
                }

        with mock.patch.object(job.stock_hist_em.fetcher, "make_request", return_value=FakeResponse()):
            result = job.fetch_qfq_hist_data(
                "300995",
                datetime.date(2023, 5, 21),
                datetime.date(2026, 5, 20),
            )

        self.assertEqual(list(result.columns), job.HIST_COLUMNS)
        self.assertNotIn("其他", result.columns)

    def test_get_market_id_supports_sh_sz_and_bj(self):
        from instock.job import adjustment_data_daily_job as job

        self.assertEqual(job.get_market_id("600519"), "1")
        self.assertEqual(job.get_market_id("300995"), "0")
        self.assertEqual(job.get_market_id("430047"), "2")
        self.assertEqual(job.get_market_id("830799"), "2")

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
            mock.patch.object(execute_daily_job, "run_adjustment_kline_repair_for_daily_job", record("adjustment")), \
            mock.patch.object(execute_daily_job.indicators_data_daily_job, "main", record("indicators")), \
            mock.patch.object(execute_daily_job.klinepattern_data_daily_job, "main", record("kline")), \
            mock.patch.object(execute_daily_job.strategy_data_daily_job, "main", record("strategy")), \
            mock.patch.object(execute_daily_job.backtest_data_daily_job, "prepare", record("backtest")), \
            mock.patch.object(execute_daily_job.basic_data_after_close_daily_job, "main", record("after_close")), \
            mock.patch.object(execute_daily_job.data_validation_daily_job, "main", record("validate")):
            execute_daily_job.main()

        self.assertLess(calls.index("other"), calls.index("adjustment"))
        self.assertLess(calls.index("adjustment"), calls.index("indicators"))
        self.assertLess(calls.index("after_close"), calls.index("validate"))

if __name__ == "__main__":
    unittest.main()
