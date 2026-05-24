import unittest
from unittest import mock


class FetchSingleStockHistTest(unittest.TestCase):
    def test_fast_fetch_keeps_f116_request_and_accepts_11_columns(self):
        from instock.job import fetch_single_stock_hist as job

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "name": "奇德新材",
                        "klines": [
                            "2026-05-20,84.84,86.10,87.20,83.90,12345,456789,4.50,-7.79,-7.25,3.21"
                        ],
                    }
                }

        with mock.patch("instock.core.crawling.stock_hist_em.fetcher.make_request", return_value=FakeResponse()) as make_request:
            data, stock_name = job.fetch_single_stock_history_fast("300995", "2026-05-20", "2026-05-20")

        self.assertEqual(stock_name, "奇德新材")
        self.assertEqual(data.loc[0, "收盘"], 86.10)
        request_params = make_request.call_args.kwargs["params"]
        self.assertEqual(request_params["fields2"], "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116")

    def test_fast_fetch_accepts_optional_f116_column(self):
        from instock.job import fetch_single_stock_hist as job

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "name": "奇德新材",
                        "klines": [
                            "2026-05-20,84.84,86.10,87.20,83.90,12345,456789,4.50,-7.79,-7.25,3.21,extra"
                        ],
                    }
                }

        with mock.patch("instock.core.crawling.stock_hist_em.fetcher.make_request", return_value=FakeResponse()):
            data, _ = job.fetch_single_stock_history_fast("300995", "2026-05-20", "2026-05-20")

        self.assertEqual(list(data.columns), job.KLINE_COLUMNS)
        self.assertNotIn("其他", data.columns)

    def test_get_market_id_supports_sh_sz_and_bj(self):
        from instock.job import fetch_single_stock_hist as job

        self.assertEqual(job.get_market_id("600519"), "1")
        self.assertEqual(job.get_market_id("300995"), "0")
        self.assertEqual(job.get_market_id("430047"), "2")
        self.assertEqual(job.get_market_id("830799"), "2")


if __name__ == "__main__":
    unittest.main()
