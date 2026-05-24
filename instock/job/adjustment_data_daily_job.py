#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
Repair ex-dividend stock K-line data before indicator calculation.

The daily spot table stores more than OHLCV data. This job only updates K-line
fields for stocks with ex-dividend records, so valuation, industry and financial
snapshot fields in cn_stock_spot are preserved.
"""

import datetime
import logging
import os.path
import sys
from typing import List, Tuple

import pandas as pd

cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.core.tablestructure as tbs
from instock.config.delay_manager import sleep_with_delay
from instock.core.crawling import stock_hist_em
import instock.lib.database as mdb
import instock.lib.run_template as runt
import instock.lib.trade_time as trd

KLINE_UPDATE_COLUMNS = (
    "open_price",
    "new_price",
    "high_price",
    "low_price",
    "volume",
    "deal_amount",
    "amplitude",
    "change_rate",
    "ups_downs",
    "turnoverrate",
)

EX_DIVIDEND_LOOKBACK_DAYS = int(os.environ.get("INSTOCK_EX_DIVIDEND_LOOKBACK_DAYS", "30"))

HIST_COLUMN_MAPPING = {
    "日期": "date",
    "开盘": "open_price",
    "收盘": "new_price",
    "最高": "high_price",
    "最低": "low_price",
    "成交量": "volume",
    "成交额": "deal_amount",
    "振幅": "amplitude",
    "涨跌幅": "change_rate",
    "涨跌额": "ups_downs",
    "换手率": "turnoverrate",
}


def _to_date(value) -> datetime.date:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"Unsupported date value: {value!r}")


def get_repair_window(run_date) -> Tuple[datetime.date, datetime.date]:
    end_date = _to_date(run_date)
    start_date_str, _ = trd.get_trade_hist_interval(end_date.strftime("%Y-%m-%d"))
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d").date()
    return start_date, end_date


def get_ex_dividend_query_window(
    repair_start_date: datetime.date,
    end_date: datetime.date,
    lookback_days: int = EX_DIVIDEND_LOOKBACK_DAYS,
) -> Tuple[datetime.date, datetime.date]:
    candidate_start_date = end_date - datetime.timedelta(days=lookback_days)
    if candidate_start_date < repair_start_date:
        candidate_start_date = repair_start_date
    return candidate_start_date, end_date


def get_ex_dividend_stocks(start_date: datetime.date, end_date: datetime.date) -> List[Tuple[str, str]]:
    table_name = tbs.TABLE_CN_STOCK_BONUS["name"]
    if not mdb.checkTableIsExist(table_name):
        logging.info("cn_stock_bonus does not exist, skip ex-dividend K-line repair")
        return []

    sql = f"""
        SELECT
            `code`,
            COALESCE(MAX(`name`), '') AS `name`
        FROM `{table_name}`
        WHERE `ex_dividend_date` IS NOT NULL
          AND `ex_dividend_date` >= %s
          AND `ex_dividend_date` <= %s
        GROUP BY `code`
        ORDER BY MIN(`ex_dividend_date`) ASC
    """
    rows = mdb.executeSqlFetch(sql, (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
    if not rows:
        return []
    return [(str(code), str(name or "")) for code, name in rows]


def fetch_qfq_hist_data(code: str, start_date: datetime.date, end_date: datetime.date) -> pd.DataFrame:
    return stock_hist_em.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date.strftime("%Y%m%d"),
        end_date=end_date.strftime("%Y%m%d"),
        adjust="qfq",
    )


def normalize_qfq_hist_data(hist_data: pd.DataFrame, code: str) -> pd.DataFrame:
    columns = ["date", "code", *KLINE_UPDATE_COLUMNS]
    if hist_data is None or hist_data.empty:
        return pd.DataFrame(columns=columns)

    data = hist_data.rename(columns=HIST_COLUMN_MAPPING).copy()
    data["code"] = code

    available_columns = [column for column in columns if column in data.columns]
    data = data[available_columns]

    missing_columns = [column for column in columns if column not in data.columns]
    for column in missing_columns:
        data[column] = None

    data = data[columns]
    data["date"] = pd.to_datetime(data["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    for column in KLINE_UPDATE_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    return data.dropna(subset=["date", "code"])


def build_update_sql(table_name: str = tbs.TABLE_CN_STOCK_SPOT["name"]) -> str:
    set_clause = ", ".join([f"`{column}` = %s" for column in KLINE_UPDATE_COLUMNS])
    return f"UPDATE `{table_name}` SET {set_clause} WHERE `code` = %s AND `date` = %s"


def _db_value(value):
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, "item") else value


def build_update_params(row: pd.Series) -> Tuple:
    values = tuple(_db_value(row[column]) for column in KLINE_UPDATE_COLUMNS)
    return (*values, str(row["code"]), str(row["date"]))


def update_cn_stock_spot_kline_data(data: pd.DataFrame) -> int:
    if data is None or data.empty:
        return 0

    table_name = tbs.TABLE_CN_STOCK_SPOT["name"]
    if not mdb.checkTableIsExist(table_name):
        logging.warning("cn_stock_spot does not exist, skip ex-dividend K-line update")
        return 0

    sql = build_update_sql(table_name)
    params = [build_update_params(row) for _, row in data.iterrows()]

    with mdb.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(sql, params)
            return cursor.rowcount


def repair_one_stock(code: str, name: str, start_date: datetime.date, end_date: datetime.date) -> int:
    logging.info(f"Repair qfq K-line data: {code} {name} ({start_date} to {end_date})")
    hist_data = fetch_qfq_hist_data(code, start_date, end_date)
    normalized_data = normalize_qfq_hist_data(hist_data, code)
    if normalized_data.empty:
        logging.warning(f"No qfq K-line data fetched for {code} {name}")
        return 0
    return update_cn_stock_spot_kline_data(normalized_data)


def repair_ex_dividend_kline_data(run_date):
    start_date, end_date = get_repair_window(run_date)
    ex_dividend_start_date, ex_dividend_end_date = get_ex_dividend_query_window(start_date, end_date)
    stocks = get_ex_dividend_stocks(ex_dividend_start_date, ex_dividend_end_date)
    if not stocks:
        logging.info(f"No ex-dividend stocks found from {ex_dividend_start_date} to {ex_dividend_end_date}")
        return

    logging.info(
        f"Found {len(stocks)} ex-dividend stocks from {ex_dividend_start_date} to {ex_dividend_end_date}, "
        f"repair qfq K-line window {start_date} to {end_date}"
    )
    success_count = 0
    updated_rows = 0

    for index, (code, name) in enumerate(stocks, 1):
        try:
            row_count = repair_one_stock(code, name, start_date, end_date)
            updated_rows += row_count
            success_count += 1
            logging.info(f"[{index}/{len(stocks)}] repaired {code} {name}, updated rows: {row_count}")
        except Exception as e:
            logging.error(f"[{index}/{len(stocks)}] repair failed: {code} {name}: {e}", exc_info=True)
        finally:
            if index < len(stocks):
                sleep_with_delay("normal")

    logging.info(
        f"Ex-dividend K-line repair finished: stocks={len(stocks)}, "
        f"success={success_count}, updated_rows={updated_rows}"
    )


def main():
    from instock.job.task_utils import log_task_start

    log_task_start("adjustment_kline_repair", "修复除权股票前复权K线数据")
    runt.run_with_args(repair_ex_dividend_kline_data)


if __name__ == "__main__":
    from instock.lib.logger_config import setup_job_logging

    setup_job_logging()
    main()
