#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
"""
按日期验证每日数据表是否写入数据。

该任务可以独立执行，也可以由 execute_daily_job 在完整流程结束后调用。

使用方式：
- python instock/job/data_validation_daily_job.py
- python instock/job/data_validation_daily_job.py 2026-05-20
- python instock/job/data_validation_daily_job.py 2026-05-20,2026-05-21
- python instock/job/data_validation_daily_job.py 2026-05-01 2026-05-20
"""

import datetime
import logging
import os.path
import sys

import pandas as pd
from sqlalchemy import text

# 兼容直接运行脚本。
cpath_current = os.path.dirname(os.path.dirname(__file__))
cpath = os.path.abspath(os.path.join(cpath_current, os.pardir))
sys.path.append(cpath)

import instock.lib.database as mdb
import instock.lib.run_template as runt


MAIN_TABLES_TO_CHECK = (
    ("cn_stock_spot", "股票基础数据"),
    ("cn_etf_spot", "ETF基础数据"),
    ("cn_stock_selection", "综合选股数据"),
    ("cn_stock_pattern", "K线形态数据"),
)

STRATEGY_TABLES_TO_CHECK = (
    ("cn_stock_strategy_turtle_trade", "海龟交易策略"),
    ("cn_stock_strategy_parking_apron", "停车信号策略"),
    ("cn_stock_strategy_backtrace_ma250", "MA250回踩策略"),
)

ALLOWED_TABLE_NAMES = {table_name for table_name, _ in MAIN_TABLES_TO_CHECK + STRATEGY_TABLES_TO_CHECK}


def _to_date(value) -> datetime.date:
    """将 run_template 传入的日期统一转换为 datetime.date。"""
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        return datetime.date.fromisoformat(value)
    raise ValueError(f"Unsupported date value: {value!r}")


def count_table_rows(table_name: str, date_str: str, require_rate_1: bool = False) -> int:
    """查询指定表在指定日期的记录数。"""
    if table_name not in ALLOWED_TABLE_NAMES:
        raise ValueError(f"Unsupported validation table: {table_name}")

    sql = f"SELECT COUNT(*) as count FROM `{table_name}` WHERE `date` = :date"
    if require_rate_1:
        sql = f"{sql} AND `rate_1` IS NOT NULL"
    df = pd.read_sql(sql=text(sql), con=mdb.engine(), params={"date": date_str})
    return int(df["count"].iloc[0])


def validate_basic_table(table_name: str, description: str, date_str: str) -> None:
    """验证普通每日数据表在指定日期是否有数据。"""
    if not mdb.checkTableIsExist(table_name):
        logging.warning(f"⚠️  表 {table_name} 不存在")
        return

    try:
        record_count = count_table_rows(table_name, date_str)
        if record_count > 0:
            logging.info(f"✅ {description} ({table_name}): {date_str} 有 {record_count} 条记录")
        else:
            logging.info(f"⚠️  {description} ({table_name}): {date_str} 暂无数据（可能是非交易日或任务未完成）")
    except Exception as e:
        logging.error(f"❌ 查询 {description} ({table_name}) 时出错: {e}")


def validate_strategy_table(table_name: str, description: str, date_str: str) -> None:
    """验证策略表在指定日期是否有数据，并统计收益率字段是否已生成。"""
    if not mdb.checkTableIsExist(table_name):
        logging.warning(f"⚠️  表 {table_name} 不存在")
        return

    try:
        record_count = count_table_rows(table_name, date_str)
        if record_count > 0:
            rates_count = count_table_rows(table_name, date_str, require_rate_1=True)
            logging.info(
                f"✅ {description} ({table_name}): {date_str} 有 {record_count} 条记录，"
                f"其中 {rates_count} 条有收益率数据"
            )
        else:
            logging.info(f"⚠️  {description} ({table_name}): {date_str} 暂无数据（可能是非交易日或策略未触发）")
    except Exception as e:
        logging.error(f"❌ 查询 {description} ({table_name}) 时出错: {e}")


def validate_daily_table_data(run_date) -> None:
    """验证指定日期的主要每日数据表是否有数据。"""
    target_date = _to_date(run_date)
    date_str = target_date.strftime("%Y-%m-%d")

    logging.info(f"🔍 开始验证 {date_str} 的数据...")

    for table_name, description in MAIN_TABLES_TO_CHECK:
        validate_basic_table(table_name, description, date_str)

    for table_name, description in STRATEGY_TABLES_TO_CHECK:
        validate_strategy_table(table_name, description, date_str)


def main() -> None:
    """脚本入口，复用 run_template 支持无参数、单日、多日和日期区间。"""
    from instock.job.task_utils import log_task_start

    log_task_start("daily_data_validation", "验证指定日期的数据表是否有数据")
    runt.run_with_args(validate_daily_table_data)


if __name__ == "__main__":
    from instock.lib.logger_config import setup_job_logging

    setup_job_logging()
    main()
