import datetime
import json
import unittest
from unittest import mock


class HistKlineFieldsTest(unittest.TestCase):
    def test_stock_code_map_migrates_old_cache_format(self):
        from instock.core.crawling import stock_hist_em

        cache_payload = json.dumps(
            {
                "cache_time": datetime.datetime.now().isoformat(),
                "code_map": {"300995": 0},
            }
        )

        with mock.patch.object(stock_hist_em.os.path, "exists", return_value=True), \
            mock.patch("builtins.open", mock.mock_open(read_data=cache_payload)):
            code_map = stock_hist_em.code_id_map_em()

        self.assertEqual(code_map["300995"]["market_id"], 0)
        self.assertEqual(code_map["300995"]["market_code"], "SZ")

    def test_stock_hist_em_keeps_f116_request_and_accepts_11_columns(self):
        from instock.core.crawling import stock_hist_em

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "klines": [
                            "2026-05-20,84.84,86.10,87.20,83.90,12345,456789,4.50,-7.79,-7.25,3.21"
                        ]
                    }
                }

        with mock.patch.object(stock_hist_em, "code_id_map_em", return_value={"300995": "0"}), \
            mock.patch.object(stock_hist_em.fetcher, "make_request", return_value=FakeResponse()) as make_request:
            data = stock_hist_em.stock_zh_a_hist(
                symbol="300995",
                start_date="20260520",
                end_date="20260520",
                adjust="qfq",
            )

        self.assertEqual(data.loc[0, "收盘"], 86.10)
        request_params = make_request.call_args.kwargs["params"]
        self.assertEqual(request_params["fields2"], "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116")

    def test_stock_hist_em_accepts_optional_f116_column(self):
        from instock.core.crawling import stock_hist_em

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "klines": [
                            "2026-05-20,84.84,86.10,87.20,83.90,12345,456789,4.50,-7.79,-7.25,3.21,extra"
                        ]
                    }
                }

        with mock.patch.object(stock_hist_em, "code_id_map_em", return_value={"300995": "0"}), \
            mock.patch.object(stock_hist_em.fetcher, "make_request", return_value=FakeResponse()):
            data = stock_hist_em.stock_zh_a_hist(
                symbol="300995",
                start_date="20260520",
                end_date="20260520",
                adjust="qfq",
            )

        self.assertEqual(list(data.columns), stock_hist_em.KLINE_DAILY_COLUMNS)
        self.assertNotIn("其他", data.columns)

    def test_fund_etf_hist_em_keeps_f116_request_and_accepts_11_columns(self):
        from instock.core.crawling import fund_etf_em

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "klines": [
                            "2026-05-20,1.11,1.12,1.13,1.10,12345,456789,1.50,0.90,0.01,2.21"
                        ]
                    }
                }

        with mock.patch.object(fund_etf_em, "_fund_etf_code_id_map_em", return_value={"159707": "0"}), \
            mock.patch.object(fund_etf_em.fetcher, "make_request", return_value=FakeResponse()) as make_request:
            data = fund_etf_em.fund_etf_hist_em(
                symbol="159707",
                start_date="20260520",
                end_date="20260520",
                adjust="qfq",
            )

        self.assertEqual(data.loc[0, "收盘"], 1.12)
        request_params = make_request.call_args.kwargs["params"]
        self.assertEqual(request_params["fields2"], "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116")

    def test_fund_etf_hist_em_accepts_optional_f116_column(self):
        from instock.core.crawling import fund_etf_em

        class FakeResponse:
            def json(self):
                return {
                    "data": {
                        "klines": [
                            "2026-05-20,1.11,1.12,1.13,1.10,12345,456789,1.50,0.90,0.01,2.21,extra"
                        ]
                    }
                }

        with mock.patch.object(fund_etf_em, "_fund_etf_code_id_map_em", return_value={"159707": "0"}), \
            mock.patch.object(fund_etf_em.fetcher, "make_request", return_value=FakeResponse()):
            data = fund_etf_em.fund_etf_hist_em(
                symbol="159707",
                start_date="20260520",
                end_date="20260520",
                adjust="qfq",
            )

        self.assertEqual(list(data.columns), fund_etf_em.KLINE_DAILY_COLUMNS)
        self.assertNotIn("其他", data.columns)


if __name__ == "__main__":
    unittest.main()
