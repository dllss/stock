#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据库读取 + 批量加载历史K线数据"""

import os
import sys
import logging
import pandas as pd

# 项目根路径
CPATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
if CPATH not in sys.path:
    sys.path.insert(0, CPATH)

import instock.lib.database as mdb


def get_latest_trade_date():
    """获取最新交易日"""
    sql = "SELECT MAX(`date`) as max_date FROM cn_stock_spot"
    df = pd.read_sql(sql, con=mdb.engine())
    return df.iloc[0]['max_date']


def get_all_stock_codes(latest_date):
    """
    获取指定交易日的所有股票列表

    Returns:
        DataFrame: 含 code/name/industry/new_price/change_rate/volume/turnoverrate/total_market_cap
    """
    sql = f"""
        SELECT `code`, `name`, `industry`, `new_price`, `change_rate`,
               `volume`, `turnoverrate`, `total_market_cap`
        FROM cn_stock_spot
        WHERE `date` = '{latest_date}'
    """
    df = pd.read_sql(sql, con=mdb.engine())
    return df


def batch_load_hist_data(stock_codes, date_start, date_end, batch_size=500):
    """
    批量加载历史K线数据

    Args:
        stock_codes: 股票代码列表
        date_start: 起始日期 (YYYY-MM-DD)
        date_end: 结束日期
        batch_size: 每批数量

    Returns:
        DataFrame: 含 code/date/open/close/high/low/volume/amount/p_change/turnover/amplitude
    """
    all_data_list = []
    total = len(stock_codes)

    for i in range(0, total, batch_size):
        batch = stock_codes[i:i + batch_size]
        codes_str = ','.join([f"'{c}'" for c in batch])

        sql = f"""
            SELECT
                `code`,
                `date`,
                `open_price` as `open`,
                `new_price` as `close`,
                `high_price` as `high`,
                `low_price` as `low`,
                `volume`,
                `deal_amount` as `amount`,
                `change_rate` as `p_change`,
                `turnoverrate` as `turnover`,
                `amplitude`
            FROM cn_stock_spot
            WHERE `code` IN ({codes_str})
              AND `date` >= '{date_start}'
              AND `date` <= '{date_end}'
            ORDER BY `code` ASC, `date` ASC
        """

        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size
        logging.info(f"  \u52a0\u8f7d\u5386\u53f2\u6570\u636e: \u7b2c {batch_num}/{total_batches} \u6279 ({len(batch)} \u53ea)...")

        batch_data = pd.read_sql(sql, con=mdb.engine())
        if batch_data is not None and len(batch_data) > 0:
            all_data_list.append(batch_data)

    if not all_data_list:
        return pd.DataFrame()

    result = pd.concat(all_data_list, ignore_index=True)
    if 'date' in result.columns:
        result['date'] = pd.to_datetime(result['date']).dt.strftime('%Y-%m-%d')
    return result


def get_hist_date_range(latest_date, lookback_days=90):
    """
    获取历史数据日期范围（往前推N个交易日）

    Returns:
        (date_start_str, date_end_str)
    """
    date_end_str = str(latest_date)
    sql_dates = f"""
        SELECT DISTINCT `date` FROM cn_stock_spot
        WHERE `date` <= '{date_end_str}'
        ORDER BY `date` DESC LIMIT {lookback_days}
    """
    dates_df = pd.read_sql(sql_dates, con=mdb.engine())
    date_start_str = str(dates_df.iloc[-1]['date'])
    return date_start_str, date_end_str


def filter_stocks(stocks_df, params):
    """
    过滤股票列表：去ST/退市、价格范围、成交量>0

    Returns:
        过滤后的 DataFrame
    """
    stocks_df = stocks_df[~stocks_df['name'].str.contains('ST|\u9000\u5e02', na=False)]
    stocks_df = stocks_df[(stocks_df['new_price'] >= params['min_price']) &
                          (stocks_df['new_price'] <= params['max_price'])]
    stocks_df = stocks_df[stocks_df['volume'] > 0]
    return stocks_df
