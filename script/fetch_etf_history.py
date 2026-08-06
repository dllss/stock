#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF历史K线数据抓取脚本
======================
功能：从东方财富抓取指定ETF的历史日线数据并存入数据库

数据内容：
- 日期、代码、名称
- 开盘价、收盘价、最高价、最低价
- 成交量、成交额、振幅、涨跌幅、涨跌额、换手率

数据来源：东方财富网 push2his.eastmoney.com

入库表：fund_etf_hist_em
主键：(code, date)

运行方式：
    # 默认抓取列表中的ETF，近2年数据
    python script/fetch_etf_history.py

    # 指定ETF代码
    python script/fetch_etf_history.py 512890 513300

    # 指定日期区间
    python script/fetch_etf_history.py 512890 513300 20240101 20260721
"""

import sys
import os
import logging

# 确保项目根目录在 sys.path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import VARCHAR
import instock.core.crawling.fund_etf_em as fee
import instock.lib.database as mdb
import instock.core.tablestructure as tbs

__author__ = 'user'
__date__ = '2026/07/21'

# 默认要抓取的ETF列表 (代码, 名称, 成立日期)
DEFAULT_ETFS = [
    ("512890", "红利低波ETF华泰柏瑞", "20181219"),
    ("513300", "纳斯达克ETF华夏", "20201022"),
    ("513500", "标普500ETF博时", "20131205"),
]


def save_etf_history(code, name, start_date="20240101", end_date="20260721"):
    """
    抓取单只ETF历史K线数据并存入数据库

    Args:
        code: ETF代码（纯数字，如 "512890"）
        name: ETF名称（如 "红利低波ETF华泰柏瑞"）
        start_date: 开始日期 YYYYMMDD
        end_date: 结束日期 YYYYMMDD

    Returns:
        int: 入库记录数，失败返回0
    """
    logging.info(f"开始抓取 ETF {code} {name} 历史数据 ({start_date} ~ {end_date})")

    try:
        # ==================== 步骤1: 抓取历史K线 ====================
        df = fee.fund_etf_hist_em(
            symbol=code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )

        if df is None or len(df) == 0:
            logging.warning(f"⚠️ {code} {name} 无数据返回")
            return 0

        logging.info(f"   原始数据: {len(df)} 条, 列: {df.columns.tolist()}")

        # ==================== 步骤2: 列名映射 ====================
        # fund_etf_hist_em 返回中文列名，映射为数据库英文字段
        col_map = {
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
            "振幅": "amplitude",
            "涨跌幅": "quote_change",
            "涨跌额": "ups_downs",
            "换手率": "turnover",
        }
        # 只重命名当前 DataFrame 中存在的列
        rename_dict = {k: v for k, v in col_map.items() if k in df.columns}
        df.rename(columns=rename_dict, inplace=True)

        # ==================== 步骤3: 补充 code / name ====================
        df.insert(0, "code", code)
        df.insert(1, "name", name)

        # ==================== 步骤4: 数据清洗 ====================
        # 处理可能的空值
        numeric_cols = ["open", "close", "high", "low", "volume",
                        "amount", "quote_change", "ups_downs"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # ==================== 步骤5: 入库 ====================
        table_name = tbs.CN_STOCK_HIST_DATA["name"]  # fund_etf_hist_em

        # 首次运行自动建表
        if not mdb.checkTableIsExist(table_name):
            cols_type = tbs.get_field_types({"code": {"type": VARCHAR(6)},
                                             "name": {"type": VARCHAR(50)},
                                             **tbs.CN_STOCK_HIST_DATA["columns"]})
            logging.info(f"📋 表 {table_name} 不存在，将创建新表")
        else:
            cols_type = None

        mdb.insert_db_from_df(
            data=df,
            table_name=table_name,
            cols_type=cols_type,
            write_index=False,
            primary_keys="`code`,`date`",
        )

        logging.info(f"✅ {code} {name}: {len(df)} 条记录已存入表 {table_name}")
        return len(df)

    except Exception as e:
        logging.error(f"❌ {code} {name} 抓取/入库失败: {e}", exc_info=True)
        return 0


def main():
    from instock.lib.logger_config import setup_job_logging
    setup_job_logging()

    # 解析命令行参数
    args = sys.argv[1:]
    codes = []
    names = []

    use_inception = False  # 是否使用各自成立日期

    if not args:
        # 无参数 → 使用默认列表（含各自成立日期）
        start_dates = {}
        codes_list = []
        names_list = []
        for code, name, inception in DEFAULT_ETFS:
            codes_list.append(code)
            names_list.append(name)
            start_dates[code] = inception
        codes, names = codes_list, names_list
        start_date = min(start_dates.values())
        end_date = "20260721"
        use_inception = True
    elif len(args) == 1:
        code = args[0]
        codes = [code]
        names = [code]
        start_date = "20240101"
        end_date = "20260721"
    elif len(args) == 2:
        codes = [args[0], args[1]]
        names = [args[0], args[1]]
        start_date = "20240101"
        end_date = "20260721"
    elif len(args) >= 4:
        codes = args[:2]
        names = args[:2]
        start_date = args[2]
        end_date = args[3]
    else:
        print("用法: python fetch_etf_history.py [代码1] [代码2] [开始日期] [结束日期]")
        sys.exit(1)

    logging.info("=" * 60)
    logging.info("ETF历史数据抓取任务开始")
    logging.info(f"目标ETF: {list(zip(codes, names))}")
    if use_inception:
        logging.info("日期区间: 各自成立日期起 ~ %s", end_date)
    else:
        logging.info("日期区间: %s ~ %s", start_date, end_date)
    logging.info("=" * 60)

    total = 0
    for code, name in zip(codes, names):
        sd = start_dates.get(code, start_date) if use_inception else start_date
        n = save_etf_history(code, name, sd, end_date)
        total += n

    logging.info("=" * 60)
    logging.info(f"任务完成，共入库 {total} 条记录")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
